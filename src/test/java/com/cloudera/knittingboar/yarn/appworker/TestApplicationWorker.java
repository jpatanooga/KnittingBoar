package com.cloudera.knittingboar.yarn.appworker;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.Utils;
import com.cloudera.knittingboar.yarn.appmaster.ApplicationMasterService;
import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;

public class TestApplicationWorker {

  private ComputableMaster<UpdateableInt> computableMaster;
  private ApplicationMasterService<UpdateableInt> masterService;
  private Future<Integer> master;
  private InetSocketAddress masterAddress;
  private ExecutorService pool;
  
  @Before
  public void setUp() throws Exception {
    masterAddress = new InetSocketAddress(9999);
    pool = Executors.newFixedThreadPool(1);

    setUpMaster();
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
  
  @After
  public void shutdown() throws Exception {
    master.get();
    pool.shutdown();
  }

  public void setUpMaster() throws Exception {
    FileSplit split = FileSplit.newBuilder()
        .setPath("testData/testWorkerService.txt").setOffset(0).setLength(200)
        .build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(200).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);

    computableMaster = new CompoundAdditionMaster();
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);

    master = pool.submit(masterService);
  }
  
  @Test
  public void testApplicationWorker() throws Exception {
    TextRecordParser<UpdateableInt> parser = new TextRecordParser<UpdateableInt>();
    CompoundAdditionWorker worker = new CompoundAdditionWorker();
    ApplicationWorker<UpdateableInt> aw = new ApplicationWorker<UpdateableInt>(parser, worker, UpdateableInt.class);
    String[] args = { "--master-addr", "localhost:9999", "--worker-id", "worker1" };

    aw.run(args);
  }
  
  public static void main(String[] args) throws Exception {
    TextRecordParser<UpdateableInt> parser = new TextRecordParser<UpdateableInt>();
    CompoundAdditionWorker worker = new CompoundAdditionWorker();
    ApplicationWorker<UpdateableInt> aw = new ApplicationWorker<UpdateableInt>(parser, worker, UpdateableInt.class);
    
    ToolRunner.run(aw, args);
  }
}
