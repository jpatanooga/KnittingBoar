package com.cloudera.knittingboar.yarn.appworker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.wovenwabbit.yarn.AvroUtils;
import com.cloudera.wovenwabbit.yarn.Updateable;
import com.cloudera.wovenwabbit.yarn.avro.generated.KnittingBoarService;
import com.cloudera.wovenwabbit.yarn.avro.generated.ProgressReport;
import com.cloudera.wovenwabbit.yarn.avro.generated.ServiceError;
import com.cloudera.wovenwabbit.yarn.avro.generated.StartupConfiguration;
import com.cloudera.wovenwabbit.yarn.avro.generated.WorkerId;

public class ApplicationWorkerService<T extends Updateable, R> implements
    Callable<Integer> {

  private static final Log LOG = LogFactory
      .getLog(ApplicationWorkerService.class);

  private enum WorkerState {
    NONE, STARTED, RUNNING, WAITING, UPDATE
  }

  private WorkerId workerId;
  private InetSocketAddress masterAddr;
  private WorkerState currentState;
  private KnittingBoarService masterService;

  private StartupConfiguration conf;

  private RecordParser<R> recordParser;
  private ComputableWorker<T, R> computable;
  private Class<T> updateable;

  private Map<String, Integer> progressCounters;
  private ProgressReport progressReport;

  private long statusSleepTime = 3000L;
  private long updateSleepTime = 1000L;
  
  private ExecutorService updateExecutor;

  class PeridoicUpdateThread implements Runnable {
    @Override
    public void run() {
      Thread.currentThread().setName("Periodic worker heartbeat thread");

      synchronized (currentState) {
        if (WorkerState.RUNNING == currentState) {
          try {
            masterService.progress(workerId, createProgressReport());
          } catch (AvroRemoteException ex) {
            LOG.warn("Encountered an exception while heartbeating to master",
                ex);
          }
        }
      }

      try {
        Thread.sleep(statusSleepTime);
      } catch (InterruptedException ex) {
        return;
      }
    }
  }

  public ApplicationWorkerService(String wid, InetSocketAddress masterAddr,
      RecordParser<R> parser, ComputableWorker<T, R> computable,
      Class<T> updateable) {

    this.workerId = AvroUtils.createWorkerId(wid);
    this.currentState = WorkerState.NONE;
    this.masterAddr = masterAddr;
    this.recordParser = parser;
    this.computable = computable;
    this.updateable = updateable;

    this.progressCounters = new HashMap<String, Integer>();
  }

  public Integer call() {
    Thread.currentThread().setName(
        "ApplicationWorkerService Thread - " + AvroUtils.getWorkerId(workerId));
    
    if (!initializeService())
      return -1;

    recordParser.setFile(conf.getSplit().getPath().toString(), conf.getSplit()
        .getOffset(), conf.getSplit().getLength());
    recordParser.parse();

    // Create an updater thread
    updateExecutor = Executors.newSingleThreadExecutor();
    updateExecutor.execute(new PeridoicUpdateThread());

    // Do some work
    currentState = WorkerState.STARTED;
    LinkedList<R> records = new LinkedList<R>();

    int countTotal = 0;
    int countCurrent = 0;
    int currentIteration = 0;

    for (currentIteration = 0; currentIteration < conf.getIterations(); currentIteration++) {
      synchronized (currentState) {
        currentState = WorkerState.RUNNING;
      }

      recordParser.reset();
      int lastUpdate = 0;

      while (recordParser.hasMoreRecords()) {
        records.add(recordParser.nextRecord());

        countTotal++;
        countCurrent++;

        synchronized (progressCounters) {
          progressCounters.put("countTotal", countTotal);
          progressCounters.put("countCurrent", countCurrent);
          progressCounters.put("currentIteration", currentIteration);
        }

        if (countCurrent == conf.getBatchSize()
            || !recordParser.hasMoreRecords()) {
          LOG.debug("Read "
              + countCurrent
              + " records, or there are no more records; computing batch result");

          T workerUpdate = computable.compute(records);

          try {
            synchronized (currentState) {
              currentState = WorkerState.UPDATE;
              masterService.update(workerId, workerUpdate.toBytes());
            }
          } catch (AvroRemoteException ex) {
            LOG.error("Unable to send update message to master", ex);
            return -1;
          }

          // Wait on master for an update
          int nextUpdate;

          try {
            LOG.info("Completed a batch, waiting on an update from master");
            nextUpdate = waitOnMasterUpdate(lastUpdate);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while waiting on master", ex);
            return -1;
          } catch (AvroRemoteException ex) {
            LOG.error("Got an error while waiting on updates from master", ex);
            return -1;
          }

          // Got an update
          try {
            ByteBuffer b = masterService.fetch(workerId, nextUpdate);
            T masterUpdate = updateable.newInstance();
            masterUpdate.fromBytes(b);
            computable.update(masterUpdate);
            lastUpdate = nextUpdate;

            LOG.debug("Requested to fetch an update from master"
                + ", workerId=" + AvroUtils.getWorkerId(workerId)
                + ", requestedUpdatedId=" + nextUpdate
                + ", responseLength=" + b.limit());
            
          } catch (AvroRemoteException ex) {
            LOG.error("Got exception while fetching an update from master", ex);
            return -1;
          } catch (Exception ex) {
            LOG.error("Got exception while processing update from master", ex);
            return -1;
          }

          countCurrent = 0;
          records.clear();
        }
      }
    }

    // We're done
    masterService.complete(workerId, createProgressReport());

    return 0;
  }

  private boolean initializeService() {
    try {
      masterService = SpecificRequestor.getClient(KnittingBoarService.class,
          new NettyTransceiver(masterAddr));

      LOG.info("Connected to master via NettyTranseiver at " + masterAddr);

    } catch (IOException ex) {
      LOG.error("Unable to connect to master at " + masterAddr);

      return false;
    }

    if (!getConfiguration())
      return false;

    return true;
  }

  private boolean getConfiguration() {
    try {
      conf = masterService.startup(workerId);

      LOG.info("Recevied startup configuration from master" + ", fileSplit=["
          + conf.getSplit().getPath() + ", " + conf.getSplit().getOffset()
          + ", " + conf.getSplit().getLength() + "]" + ", batchSize="
          + conf.getBatchSize() + ", iterations=" + conf.getIterations());

    } catch (AvroRemoteException ex) {
      if (ex instanceof ServiceError) {
        LOG.error(
            "Unable to call startup(): " + ((ServiceError) ex).getDescription(),
            ex);
      } else {
        LOG.error("Unable to call startup()", ex);
      }

      return false;
    }

    return true;
  }

  private int waitOnMasterUpdate(int lastUpdate) throws InterruptedException,
      AvroRemoteException {
    int nextUpdate = 0;
    long waitStarted = System.currentTimeMillis();
    long waitingFor = 0;

    while ((nextUpdate = masterService
        .waiting(workerId, lastUpdate, waitingFor)) < 0) {

      synchronized (currentState) {
        currentState = WorkerState.WAITING;
      }

      Thread.sleep(updateSleepTime);
      waitingFor = System.currentTimeMillis() - waitStarted;

      LOG.debug("Waiting on update from master for " + waitingFor + "ms");
    }

    return nextUpdate;
  }

  private ProgressReport createProgressReport() {
    if (progressReport == null) {
      progressReport = new ProgressReport();
      progressReport.setWorkerId(workerId);
    }

    // Create a new report
    Map<CharSequence, CharSequence> report = new HashMap<CharSequence, CharSequence>();

    synchronized (progressCounters) {
      for (Map.Entry<String, Integer> entry : progressCounters.entrySet()) {
        report.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }

    progressReport.setReport(report);

    if (LOG.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Created a progress report");
      sb.append(", workerId=").append(
          AvroUtils.getWorkerId(progressReport.getWorkerId()));

      for (Map.Entry<CharSequence, CharSequence> entry : progressReport
          .getReport().entrySet()) {
        sb.append(", ").append(entry.getKey()).append("=")
            .append(entry.getValue());
      }

      LOG.debug(sb.toString());
    }

    return progressReport;
  }
}
