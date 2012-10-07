package com.cloudera.knittingboar.yarn.appmaster;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.knittingboar.yarn.AvroUtils;
import com.cloudera.knittingboar.yarn.Updateable;
import com.cloudera.knittingboar.yarn.avro.generated.KnittingBoarService;
import com.cloudera.knittingboar.yarn.avro.generated.ProgressReport;
import com.cloudera.knittingboar.yarn.avro.generated.ServiceError;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;



/**
 * @author Michael
 * 
 *         TODO: change to ConcurrentHashMap (maybe)?
 * @param <T>
 */
public class ApplicationMasterService<T extends Updateable> implements
    KnittingBoarService, Callable<Integer> {

  private static final Log LOG = LogFactory
      .getLog(ApplicationMasterService.class);

  private enum WorkerState {
    NONE, STARTED, RUNNING, UPDATE, WAITING, COMPLETE, ERROR
  }

  private enum MasterState {
    WAITING, UPDATING
  }

  private HashMap<WorkerId, StartupConfiguration> workers;
  private HashMap<WorkerId, WorkerState> workersState;
  private HashMap<WorkerId, LinkedHashMap<Long, ProgressReport>> workersProgress;
  private HashMap<WorkerId, T> workersUpdate;

  private MasterState masterState;
  private int currentUpdateId = 0;
  private HashMap<Integer, T> masterUpdates;

  private ComputableMaster<T> computable;
  private Class<T> updateable;

  private final InetSocketAddress masterAddr;
  private Server masterServer;
  private CountDownLatch workersCompleted;

  private boolean isRunning;
  private Thread ourThread;

  public ApplicationMasterService(InetSocketAddress masterAddr,
      HashMap<WorkerId, StartupConfiguration> workers,
      ComputableMaster<T> computable, Class<T> updatable) {

    if (masterAddr == null || computable == null || updatable == null)
      throw new IllegalStateException(
          "masterAddress or computeUpdate cannot be null");

    this.workers = workers;
    this.workersCompleted = new CountDownLatch(workers.size());
    this.workersState = new HashMap<WorkerId, ApplicationMasterService.WorkerState>();
    this.workersProgress = new HashMap<WorkerId, LinkedHashMap<Long, ProgressReport>>();

    this.masterState = MasterState.WAITING;
    this.masterUpdates = new HashMap<Integer, T>();
    this.masterAddr = masterAddr;
    this.computable = computable;
    this.updateable = updatable;
  }

  public Integer call() {
    Thread.currentThread().setName("ApplicationMasterService Thread");
    LOG.info("Starting MasterService [NettyServer] on " + masterAddr);

    masterServer = new NettyServer(new SpecificResponder(
        KnittingBoarService.class, this), masterAddr);

    isRunning = true;
    ourThread = Thread.currentThread();

    try {
      workersCompleted.await();

      int complete = 0;
      int error = 0;

      for (WorkerState state : workersState.values()) {
        switch (state) {
        case COMPLETE:
          complete++;
          break;

        case ERROR:
          error++;
          break;
        }
      }

      LOG.info("Shutting down master service" + ", workersComplete=" + complete
          + ", workersError=" + error);

      isRunning = false;
      masterServer.close();

      return (error == 0) ? 0 : 1;
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while waiting for workers to complete", ex);
      return -1;
    }
  }

  public void start() {
    try {
      LOG.debug("Starting ApplicationMasterService as a standalone server");

      ExecutorService pool = Executors.newSingleThreadExecutor();
      pool.submit(this).get();

    } catch (Exception ex) {
      // nadda
    }

    LOG.debug("Completed and exiting standalone server");
  }

  public void stop() {
    if (isRunning == true && ourThread != null) {
      LOG.warn("Forcefully shutting ourselves down");
      ourThread.interrupt();
    }

    LOG.info("Shutting down MasterService [NettyServer]");
    if (masterServer != null)
      masterServer.close();
  }

  @Override
  public StartupConfiguration startup(WorkerId workerId)
      throws AvroRemoteException, ServiceError {

    synchronized (workersState) {
      if (workersState.containsKey(workerId))
        throw ServiceError
            .newBuilder()
            .setDescription(
                "Worker " + AvroUtils.getWorkerId(workerId)
                    + "already registered.").build();

      StartupConfiguration conf = workers.get(workerId);

      LOG.debug("Got a startup call, workerId="
          + AvroUtils.getWorkerId(workerId) + ", responded with"
          + ", batchSize=" + conf.getBatchSize() + ", iterations="
          + conf.getIterations() + ", fileSplit=[" + conf.getSplit().getPath()
          + ", " + conf.getSplit().getOffset() + "]");

      workersState.put(workerId, WorkerState.STARTED);

      return conf;
    }
  }

  private boolean handleProgress(WorkerId workerId,
      ProgressReport report) {

    synchronized (workersState) {
      LinkedHashMap<Long, ProgressReport> progress = workersProgress
          .get(workerId);
      if (progress == null)
        progress = new LinkedHashMap<Long, ProgressReport>();
  
      progress.put(System.currentTimeMillis(), report);
      workersProgress.put(workerId, progress);
      workersState.put(workerId, WorkerState.RUNNING);
  
      LOG.debug("Got a progress report" + ", workerId="
          + AvroUtils.getWorkerId(workerId) + ", workerState="
          + workersState.get(workerId) + ", progressSize="
          + report.getReport().size() + ", totalReports=" + progress.size());
    }
    
    return true;
  }

  @Override
  public boolean progress(WorkerId workerId, ProgressReport report)
      throws AvroRemoteException {

    return handleProgress(workerId, report);
  }

  @Override
  public boolean update(WorkerId workerId, ByteBuffer data)
      throws AvroRemoteException {

    // We only want updates from workers we know are either running, or have
    // started
    WorkerState workerState;

    synchronized (workersState) {
      workerState = workersState.get(workerId);

      if (workerState != null) {
        if (workerState != WorkerState.RUNNING
            && workerState != WorkerState.STARTED) {

          LOG.debug("Received an erroneous update" + ", workerId="
              + AvroUtils.getWorkerId(workerId) + ", workerState="
              + workerState + ", length=" + data.limit());

          return false;
        }
      }
    }

    LOG.info("Received update, workerId=" + AvroUtils.getWorkerId(workerId)
        + ", workerState=" + workerState + ", length=" + data.limit());

    synchronized (masterState) {
      // First update, create a new update map
      if (MasterState.WAITING == masterState) {
        LOG.debug("Initial update for this round, initializing update map");

        if (workersUpdate == null)
          workersUpdate = new HashMap<WorkerId, T>();

        workersUpdate.clear();
        masterState = MasterState.UPDATING;
      }
    }

    // Duplicate update?
    if (workersUpdate.containsKey(workerId)) {
      LOG.warn("Received a duplicate update for, workerId="
          + AvroUtils.getWorkerId(workerId) + ", ignoring this update");

      return false;
    }

    // Add the update
    // Synchornized?

    T update;
    try {
      update = updateable.newInstance();
      update.fromBytes(data);
    } catch (Exception ex) {
      LOG.warn("Unable to instantiate a computable object", ex);
      return false;
    }

    synchronized (workersState) {
      workersUpdate.put(workerId, update);
      workersState.put(workerId, WorkerState.UPDATE);

      if (workersUpdate.size() == workersState.size()) {
        LOG.info("Received updates from all workers, spawing local compute thread");

        // Fire off thread to compute update
        Thread updateThread = new Thread(new Runnable() {

          @Override
          public void run() {
            long startTime, endTime;

            startTime = System.currentTimeMillis();
            T result = computable.compute(workersUpdate.values(),
                masterUpdates.values());
            endTime = System.currentTimeMillis();

            LOG.info("Computed local update in " + (endTime - startTime) + "ms");

            synchronized (masterUpdates) {
              currentUpdateId++;
              masterUpdates.put(currentUpdateId, result);
              masterState = MasterState.WAITING;
            }
          }
        });

        updateThread.setName("Compute thread");
        updateThread.start();
      }
    }

    return true;
  }

  @Override
  public int waiting(WorkerId workerId, int lastUpdate, long waiting)
      throws AvroRemoteException {

    synchronized (workersState) {
      workersState.put(workerId, WorkerState.WAITING);

      LOG.info("Got waiting message" + ", workerId="
          + AvroUtils.getWorkerId(workerId) + ", workerState="
          + workersState.get(workerId) + ", lastUpdate=" + lastUpdate
          + ", waitingFor=" + waiting);
    }

    if (MasterState.UPDATING == masterState && lastUpdate == currentUpdateId)
      return -1;

    return currentUpdateId;
  }

  @Override
  public ByteBuffer fetch(WorkerId workerId, int updateId)
      throws AvroRemoteException {
    
    LOG.debug("Received a fetch request"
        + ", workerId=" + AvroUtils.getWorkerId(workerId)
        + ", requestedUpdateId=" + updateId);
    
    synchronized (workersState) {
      workersState.put(workerId, WorkerState.RUNNING);
    }

    return masterUpdates.get(updateId).toBytes();
  }

  @Override
  public void complete(WorkerId workerId, ProgressReport finalReport) {
    handleProgress(workerId, finalReport);
    workersState.put(workerId, WorkerState.COMPLETE);

    LOG.info("Received complete message, workerId="
        + AvroUtils.getWorkerId(workerId));

    workersCompleted.countDown();
  }

  @Override
  public void error(WorkerId workerId, CharSequence message) {
    LOG.warn("A worker encountered an error" + ", worker="
        + AvroUtils.getWorkerId(workerId) + ", message=" + message);

    workersState.put(workerId, WorkerState.ERROR);
    workersCompleted.countDown();
  }
}
