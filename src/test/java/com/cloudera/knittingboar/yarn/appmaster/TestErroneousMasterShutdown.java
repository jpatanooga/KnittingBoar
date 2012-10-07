package com.cloudera.knittingboar.yarn.appmaster;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.cloudera.knittingboar.yarn.AvroUtils;
import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;


public class TestErroneousMasterShutdown {

  private FutureTask<Integer> master;
  private ApplicationMasterService<UpdateableInt> masterService;
  private ExecutorService pool;
  
  @Before
  public void setUp() throws Exception {
    FileSplit split = FileSplit.newBuilder().setPath("/foo/bar").setOffset(100)
        .setLength(200).build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(2).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(AvroUtils.createWorkerId("worker1"), conf);
    workers.put(AvroUtils.createWorkerId("worker2"), conf);

    InetSocketAddress masterAddress = new InetSocketAddress(9999);
    ComputableMaster<UpdateableInt> computableMaster = new CompoundAdditionMaster();
    
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);
    master = new FutureTask<Integer>(masterService);
    pool = Executors.newSingleThreadExecutor();
    pool.submit(master);
    
    Thread.sleep(1000);
  }
  
  @Test
  public void testErroneousShutdownViaStop() throws Exception
  {
    masterService.stop();
    assertEquals(Integer.valueOf(-1), master.get());
  }
  
  @Test
  public void testErroneousShutdownViaShutdown() throws Exception {
    pool.shutdownNow();
    assertEquals(Integer.valueOf(-1), master.get());
  }
}
