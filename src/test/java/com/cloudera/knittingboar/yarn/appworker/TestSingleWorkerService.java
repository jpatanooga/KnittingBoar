package com.cloudera.wovenwabbit.yarn.appworker;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.cloudera.wovenwabbit.yarn.AvroUtils;
import com.cloudera.wovenwabbit.yarn.CompoundAdditionMaster;
import com.cloudera.wovenwabbit.yarn.CompoundAdditionWorker;
import com.cloudera.wovenwabbit.yarn.UpdateableInt;
import com.cloudera.wovenwabbit.yarn.appmaster.ApplicationMasterService;
import com.cloudera.wovenwabbit.yarn.appmaster.ComputableMaster;
import com.cloudera.wovenwabbit.yarn.appworker.ApplicationWorkerService;
import com.cloudera.wovenwabbit.yarn.appworker.ComputableWorker;
import com.cloudera.wovenwabbit.yarn.appworker.HDFSLineParser;
import com.cloudera.wovenwabbit.yarn.avro.generated.FileSplit;
import com.cloudera.wovenwabbit.yarn.avro.generated.StartupConfiguration;
import com.cloudera.wovenwabbit.yarn.avro.generated.WorkerId;

public class TestSingleWorkerService {

  InetSocketAddress masterAddress;
  ExecutorService pool;

  private ApplicationMasterService<UpdateableInt> masterService;
  private FutureTask<Integer> master;
  private ComputableMaster<UpdateableInt> computableMaster;

  private ApplicationWorkerService<UpdateableInt, String> workerService;
  private FutureTask<Integer> worker;
  private ComputableWorker<UpdateableInt, String> computableWorker;

  @Before
  public void setUp() throws Exception {
    masterAddress = new InetSocketAddress(9999);
    pool = Executors.newFixedThreadPool(2);

    setUpMaster();

    setUpWorker();
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
    workers.put(AvroUtils.createWorkerId("worker1"), conf);

    computableMaster = new CompoundAdditionMaster();
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);

    master = new FutureTask<Integer>(masterService);

    pool.submit(master);
  }

  private void setUpWorker() {
    HDFSLineParser parser = new HDFSLineParser();
    computableWorker = new CompoundAdditionWorker();
    workerService = new ApplicationWorkerService<UpdateableInt, String>(
        "worker1", masterAddress, parser, computableWorker, UpdateableInt.class);

    worker = new FutureTask<Integer>(workerService);

    pool.submit(worker);
  }

  @Test
  public void testWorkerService() throws Exception {
    worker.get();
    master.get();

    // Bozo numbers
    assertEquals(Integer.valueOf(1300), computableWorker.getResults().get());
    assertEquals(Integer.valueOf(2970), computableMaster.getResults().get());
  }
}
