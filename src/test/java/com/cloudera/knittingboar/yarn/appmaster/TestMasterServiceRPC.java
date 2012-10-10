package com.cloudera.knittingboar.yarn.appmaster;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static junit.framework.Assert.*;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.Utils;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.KnittingBoarService;
import com.cloudera.knittingboar.yarn.avro.generated.ProgressReport;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;


public class TestMasterServiceRPC {

  private void assertStartupConfiguration(StartupConfiguration conf,
      String path, Integer batchSize, Integer iterations,
      Map<String, String> other) {

    assertEquals(path, conf.getSplit().getPath().toString());
    assertEquals(batchSize, conf.getBatchSize());
    assertEquals(iterations, conf.getIterations());
    assertEquals(other, conf.getOther());
  }
  
  @Test
  public void testMasterRPC() throws Exception {

    FileSplit split = FileSplit.newBuilder()
        .setPath("/foo/bar")
        .setOffset(100)
        .setLength(200)
        .build();
    
    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split)
        .setBatchSize(2)
        .setIterations(1)
        .setOther(null)
        .build();
    
    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);
    workers.put(Utils.createWorkerId("worker2"), conf);
    
    InetSocketAddress masterAddress = new InetSocketAddress(9999);
    ComputableMaster<UpdateableInt> computableMaster = new CompoundAdditionMaster();
    ApplicationMasterService<UpdateableInt> masterService = new ApplicationMasterService<UpdateableInt>(
        masterAddress, workers, computableMaster, UpdateableInt.class);

    FutureTask<Integer> master = new FutureTask<Integer>(masterService);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    pool.submit(master);

    KnittingBoarService masterPrc = SpecificRequestor.getClient(
        KnittingBoarService.class, new NettyTransceiver(masterAddress));

    // Test 
    WorkerId workerOne = Utils.createWorkerId("worker1");
    WorkerId workerTwo = Utils.createWorkerId("worker2");
    
    // Startup
    assertStartupConfiguration(masterPrc.startup(workerOne), "/foo/bar", 2, 1, null);
    assertStartupConfiguration(masterPrc.startup(workerTwo), "/foo/bar", 2, 1, null);
    
    // Progress
    HashMap<CharSequence, CharSequence> counters = new HashMap<CharSequence, CharSequence>();
    counters.put("counter1", "1");
    
    ProgressReport progress = ProgressReport.newBuilder()
        .setWorkerId(workerOne)
        .setReport(counters)
        .build();
    
    assertEquals(true, masterPrc.progress(workerOne, progress));
    assertEquals(true, masterPrc.progress(workerTwo, progress));
    
    // Waiting without an update, should come back as 0 (or last update ID)
    assertEquals(0, masterPrc.waiting(workerOne, 0, 0));
    assertEquals(0, masterPrc.waiting(workerTwo, 0, 0));
    
    // Put workers back in running state
    assertEquals(true, masterPrc.progress(workerOne, progress));
    assertEquals(true, masterPrc.progress(workerTwo, progress));
    
    // Update
    UpdateableInt uInt = new UpdateableInt();
    uInt.set(100);
    assertEquals(true, masterPrc.update(workerOne, uInt.toBytes()));
    // We should not be in RUNNING state on master, so we get a false
    assertEquals(false, masterPrc.update(workerOne, uInt.toBytes()));

    // Obnoxiously put ourself back into WORKING state, but master refuses our
    // update, because we already provided an update in this round
    assertEquals(true, masterPrc.progress(workerOne, progress));
    assertEquals(false, masterPrc.update(workerOne, uInt.toBytes()));

    // Waiting, we have a partial update in progress, should get back -1
    assertEquals(-1, masterPrc.waiting(workerOne, 0, 0));
    
    // Second update
    assertEquals(true, masterPrc.update(workerTwo, uInt.toBytes()));

    // Wait a moment to allow master to compute update
    Thread.sleep(2000);
    
    // Waiting should come back with a fetchId of 1
    assertEquals(1, masterPrc.waiting(workerOne, 0, 1));
    assertEquals(1, masterPrc.waiting(workerTwo, 0, 1));
    
    // Fetch
    UpdateableInt update = new UpdateableInt();
    update.fromBytes(masterPrc.fetch(workerOne, 1));
    assertEquals(Integer.valueOf(200), update.get());
    
    // Complete
    masterPrc.complete(workerOne, progress);
    masterPrc.error(workerTwo, "An error occurred");
    
    // Shutdown, expect return code of 1, because we have at least failure
    assertEquals(new Integer(1), master.get());
  }
}
