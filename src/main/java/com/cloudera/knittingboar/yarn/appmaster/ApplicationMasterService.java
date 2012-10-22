package com.cloudera.knittingboar.yarn.appmaster;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.Utils;
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
 * 
 *         TODO: Have an expiration period for all workers to check in,
 *         otherwise we need to bail?
 * 
 *         TODO: We need to fix the overall logic, that actually waits for
 *         workers to check in. For example, what happens if we have 2/3 workers
 *         check-in, and 1 didn't for some reason, does that mean that
 *         everything continues? Do we let our other workers start and waste
 *         their time?
 * 
 *         Additionally, what implications does that have on the RPC? Do workers
 *         poll for a command to tell them to start, or do we want to push that
 *         down to them?
 * 
 * @param <T>
 */

/*
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

  private Map<WorkerId, StartupConfiguration> workers;
  private Map<WorkerId, WorkerState> workersState;
  private Map<WorkerId, LinkedHashMap<Long, ProgressReport>> workersProgress;
  private Map<WorkerId, T> workersUpdate;

  private MasterState masterState;
  private int currentUpdateId = 0;
  private Map<Integer, T> masterUpdates;

  private ComputableMaster<T> computable;
  private Class<T> updateable;

  private final InetSocketAddress masterAddr;
  private Server masterServer;
  private CountDownLatch workersCompleted;

  private Configuration conf;
  private Map<CharSequence, CharSequence> appConf;
  
  private ExecutorService standalone;
  private Future<Integer> standaloneResult;

  // Metrics
  private Map<WorkerId, Map<CharSequence, Long>> workerMetrics;
  private long mMasterTime;
  private long mMasterExecutions;
  private long mUpdates;

  public ApplicationMasterService(InetSocketAddress masterAddr,
      Map<WorkerId, StartupConfiguration> workers,
      ComputableMaster<T> computable, Class<T> updatable,
      Map<CharSequence, CharSequence> appConf, Configuration conf) {

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
    
    this.appConf = appConf;
    this.conf = conf;
    Utils.mergeConfigs(this.appConf, this.conf);
    
    this.computable.setup(this.conf);
    
    // Merger workers into worker state
    for (WorkerId workerId : workers.keySet()) {
      workersState.put(workerId, WorkerState.NONE);
    }
  }
  
  public ApplicationMasterService(InetSocketAddress masterAddr,
      Map<WorkerId, StartupConfiguration> workers,
      ComputableMaster<T> computable, Class<T> updatable,
      Map<CharSequence, CharSequence> appConf) {

    this(masterAddr, workers, computable, updatable, appConf,
        new Configuration());
  }
  
  public ApplicationMasterService(InetSocketAddress masterAddr,
      Map<WorkerId, StartupConfiguration> workers,
      ComputableMaster<T> computable, Class<T> updatable) {

    this(masterAddr, workers, computable, updatable, null);
  }  

  public Integer call() {
    Thread.currentThread().setName("ApplicationMasterService Thread");
    LOG.info("Starting MasterService [NettyServer] on " + masterAddr);

    masterServer = new NettyServer(new SpecificResponder(
        KnittingBoarService.class, this), masterAddr);

    try {
      workersCompleted.await();

      int complete = 0;
      int error = 0;
      int unknown = 0;
      
      for (WorkerState state : workersState.values()) {
        switch (state) {
        case COMPLETE:
          complete++;
          break;

        case ERROR:
          error++;
          break;
          
        default:
          unknown++;
          break;
        }
      }
      
      // TODO: fix, and move here from ApplicationMaster
      // computable.complete(null);

      LOG.info("All workers have completed. Shutting down master service"
          + ", workersComplete=" + complete + ", workersError=" + error
          + ", workersUnknown=" + unknown);

      return (error == 0 && unknown == 0) ? 0 : 1;
    } catch (InterruptedException ex) {
      // This will occur purposely if:
      // 1. Someone calls a ExecutorService.shutdownNow()
      // 2. Via our stop() method using Future.cancel()
      LOG.warn("Interrupted while waiting for workers to complete", ex);
      return -1;
    } finally {
      LOG.debug("Shutting down Netty server");
      masterServer.close();

      printMetrics();
    }
  }

  public void start() {
    LOG.info("Starting ApplicationMasterService as a standalone server");

    standalone = Executors.newSingleThreadExecutor();
    standaloneResult = standalone.submit(this);
  }

  public void fail() {
    while (workersCompleted.getCount() > 0)
      workersCompleted.countDown();
  }
  
  public void stop() {
    LOG.info("Shutting down standalone server");

    if (standalone != null)
      standalone.shutdownNow();

    if (standaloneResult != null) {
      try {
        LOG.info("Master service completed with exit code " + standaloneResult.get(0, TimeUnit.SECONDS));
      } catch (Exception ex) {
        LOG.warn("Unable to get exit code from master service", ex);
      }
    }
    
    LOG.info("Shutting down MasterService [NettyServer]");
    if (masterServer != null)
      masterServer.close();
  }
  
  private void printMetrics() {
    StringBuffer metrics = new StringBuffer("Master metrics:\n");
    metrics.append("  MasterTime: ").append(mMasterTime).append("\n");
    metrics.append("  MasterExecutions: ").append(mMasterExecutions).append("\n");
    metrics.append("  UpdatesReceived: " ).append(mUpdates).append("\n");
    metrics.append("\n");

    for (WorkerId wid : workerMetrics.keySet()) {
      metrics.append("  Worker Metrics - ").append(Utils.getWorkerId(wid)).append(":").append("\n");
      
      for (Map.Entry<CharSequence, Long> entry : workerMetrics.get(wid).entrySet()) {
        metrics.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      }
      metrics.append("\n");
    }
    
    LOG.info(metrics.toString());
  }

  @Override
  public StartupConfiguration startup(WorkerId workerId)
      throws AvroRemoteException, ServiceError {

    synchronized (workersState) {
      if (!workersState.containsKey(workerId)) {
        throw ServiceError
            .newBuilder()
            .setDescription(
                "Worker " + Utils.getWorkerId(workerId)
                    + "unknown.").build();
      }
      
      // TODO: can a worker "start" more than once?
      
      StartupConfiguration workerConf = workers.get(workerId);
      Utils.mergeConfigs(appConf, workerConf);

      LOG.debug("Got a startup call, workerId="
          + Utils.getWorkerId(workerId) + ", responded with"
          + ", batchSize=" + workerConf.getBatchSize() + ", iterations="
          + workerConf.getIterations() + ", fileSplit=[" + workerConf.getSplit().getPath()
          + ", " + workerConf.getSplit().getOffset() + "]");

      workersState.put(workerId, WorkerState.STARTED);

      return workerConf;
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
          + Utils.getWorkerId(workerId) + ", workerState="
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
              + Utils.getWorkerId(workerId) + ", workerState="
              + workerState + ", length=" + data.limit());

          return false;
        }
      }
    }

    LOG.info("Received update, workerId=" + Utils.getWorkerId(workerId)
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
          + Utils.getWorkerId(workerId) + ", ignoring this update");

      return false;
    }

    // Add the update
    // Synchornized?

    T update;
    try {
      update = updateable.newInstance();
      data.rewind();
      update.fromBytes(data);
    } catch (Exception ex) {
      LOG.warn("Unable to instantiate a computable object", ex);
      return false;
    }

    synchronized (workersState) {
      workersUpdate.put(workerId, update);
      workersState.put(workerId, WorkerState.UPDATE);

      // Our latch should have the number of currently active workers
      if (workersUpdate.size() == workersCompleted.getCount()) {
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
            
            mMasterExecutions++;
            mMasterTime += (endTime - startTime);

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

    mUpdates++;
    return true;
  }

  @Override
  public int waiting(WorkerId workerId, int lastUpdate, long waiting)
      throws AvroRemoteException {

    synchronized (workersState) {
      workersState.put(workerId, WorkerState.WAITING);

      LOG.info("Got waiting message" + ", workerId="
          + Utils.getWorkerId(workerId) + ", workerState="
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
        + ", workerId=" + Utils.getWorkerId(workerId)
        + ", requestedUpdateId=" + updateId);
    
    synchronized (workersState) {
      workersState.put(workerId, WorkerState.RUNNING);
    }

    ByteBuffer bytes = masterUpdates.get(updateId).toBytes();
    bytes.rewind();
    
    return bytes;
  }

  @Override
  public void complete(WorkerId workerId, ProgressReport finalReport) {
    handleProgress(workerId, finalReport);
    workersState.put(workerId, WorkerState.COMPLETE);

    LOG.info("Received complete message, workerId="
        + Utils.getWorkerId(workerId));

    workersCompleted.countDown();
  }

  @Override
  public void error(WorkerId workerId, CharSequence message) {
    LOG.warn("A worker encountered an error" + ", worker="
        + Utils.getWorkerId(workerId) + ", message=" + message);

    workersState.put(workerId, WorkerState.ERROR);
    workersCompleted.countDown();
  }

  @Override
  public void metricsReport(WorkerId workerId, Map<CharSequence, Long> metrics) {
    if (workerMetrics == null)
      workerMetrics = new HashMap<WorkerId, Map<CharSequence, Long>>();
    
    // Bludgeon it for now, TODO: be smarter about merging them
    workerMetrics.put(workerId, metrics);
  }
}