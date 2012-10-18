package com.cloudera.knittingboar.yarn.appworker;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.Utils;
import com.cloudera.knittingboar.yarn.appmaster.ApplicationMasterService;
import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;

public class TestMultipleWorkerServices {

  InetSocketAddress masterAddress;
  ExecutorService pool;

  private ApplicationMasterService<UpdateableInt> masterService;
  private Future<Integer> master;
  private ComputableMaster<UpdateableInt> computableMaster;

  private ArrayList<ApplicationWorkerService<UpdateableInt>> workerServices = new ArrayList<ApplicationWorkerService<UpdateableInt>>();
  private ArrayList<Future<Integer>> workers = new ArrayList<Future<Integer>>();
  private ArrayList<ComputableWorker<UpdateableInt>> computableWorkers = new ArrayList<ComputableWorker<UpdateableInt>>();

  @Before
  public void setUp() throws Exception {
    masterAddress = new InetSocketAddress(9999);
    pool = Executors.newFixedThreadPool(4);

    setUpMaster();

    setUpWorker("worker1");
    setUpWorker("worker2");
    setUpWorker("worker3");
  }

  @Before
  public void setUpFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path testDir = new Path("testData");
    Path inputFile = new Path(testDir, "testWorkerService.txt");

    Writer writer = new OutputStreamWriter(localFs.create(inputFile, true));
    writer.write("10\n20\n30\n40\n50\n60\n70\n80\n90\n100");
    writer.close();
  }

  public void setUpMaster() throws Exception {
    FileSplit split = FileSplit.newBuilder()
        .setPath("testData/testWorkerService.txt").setOffset(0).setLength(200)
        .build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(2).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);
    workers.put(Utils.createWorkerId("worker2"), conf);
    workers.put(Utils.createWorkerId("worker3"), conf);

    computableMaster = new CompoundAdditionMaster();
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);

    master = pool.submit(masterService);
  }

  private void setUpWorker(String name) {
    TextRecordParser<UpdateableInt> parser = new TextRecordParser<UpdateableInt>();
    ComputableWorker<UpdateableInt> computableWorker = new CompoundAdditionWorker();
    final ApplicationWorkerService<UpdateableInt> workerService = new ApplicationWorkerService<UpdateableInt>(
        name, masterAddress, parser, computableWorker, UpdateableInt.class);

    Future<Integer> worker = pool.submit(new Callable<Integer>() {
      public Integer call() {
        return workerService.run();
      }
    });

    computableWorkers.add(computableWorker);
    workerServices.add(workerService);
    workers.add(worker);
  }

  @Test
  public void testWorkerService() throws Exception {
    workers.get(0).get();
    workers.get(1).get();
    workers.get(2).get();
    master.get();

    // Needed, to make sure we shut down correctly...
    Thread.currentThread().join();

    // Bozo numbers
    //assertEquals(Integer.valueOf(12100), computableWorkers.get(0).getResults().get());
    //assertEquals(Integer.valueOf(12100), computableWorkers.get(1).getResults().get());
    //assertEquals(Integer.valueOf(12100), computableWorkers.get(2).getResults().get());
    //assertEquals(Integer.valueOf(51570), computableMaster.getResults().get());
  }
}
