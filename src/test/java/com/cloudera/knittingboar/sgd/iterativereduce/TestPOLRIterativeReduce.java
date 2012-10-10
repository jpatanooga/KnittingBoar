package com.cloudera.knittingboar.sgd.iterativereduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.yarn.AvroUtils;
import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;
//import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.appmaster.ApplicationMasterService;
import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;
import com.cloudera.knittingboar.yarn.appworker.ApplicationWorkerService;
import com.cloudera.knittingboar.yarn.appworker.ComputableWorker;
import com.cloudera.knittingboar.yarn.appworker.HDFSLineParser;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;


public class TestPOLRIterativeReduce {

  InetSocketAddress masterAddress;
  ExecutorService pool;

  private ApplicationMasterService<ParameterVectorGradientUpdatable> masterService;
  private FutureTask<Integer> master;
  private ComputableMaster<ParameterVectorGradientUpdatable> computableMaster;

  private ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable, String>> workerServices = new ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable, String>>();
  private ArrayList<FutureTask<Integer>> workers = new ArrayList<FutureTask<Integer>>();
  private ArrayList<ComputableWorker<ParameterVectorGradientUpdatable, String>> computableWorkers = new ArrayList<ComputableWorker<ParameterVectorGradientUpdatable, String>>();

  
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train3/"));  
      
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 1);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");
    
    return c;
    
  }    
  
  
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
/* 
    Path testDir = new Path("testData");
    Path inputFile = new Path(testDir, "testWorkerService.txt");

    Writer writer = new OutputStreamWriter(localFs.create(inputFile, true));
    writer.write("10\n20\n30\n40\n50\n60\n70\n80\n90\n100");
    writer.close();
    */
  }

  public void setUpMaster() throws Exception {
    
    // /Users/jpatterson/Downloads/datasets/20news-kboar/train3/kboar-shard-0.txt
    
    FileSplit split = FileSplit.newBuilder()
        .setPath("/Users/jpatterson/Downloads/datasets/20news-kboar/train3/kboar-shard-0.txt").setOffset(0).setLength(834889)
        .build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(200).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(AvroUtils.createWorkerId("worker1"), conf);
    workers.put(AvroUtils.createWorkerId("worker2"), conf);
    workers.put(AvroUtils.createWorkerId("worker3"), conf);

    //computableMaster = new CompoundAdditionMaster();
    computableMaster = new POLRMasterNode();
    masterService = new ApplicationMasterService<ParameterVectorGradientUpdatable>(masterAddress,
        workers, computableMaster, ParameterVectorGradientUpdatable.class, null, generateDebugConfigurationObject() );

    master = new FutureTask<Integer>(masterService);

    pool.submit(master);
  }

  private void setUpWorker(String name) {
    HDFSLineParser parser = new HDFSLineParser();
    ComputableWorker<ParameterVectorGradientUpdatable, String> computableWorker = new POLRWorkerNode();
    ApplicationWorkerService<ParameterVectorGradientUpdatable, String> workerService = new ApplicationWorkerService<ParameterVectorGradientUpdatable, String>(
        name, masterAddress, parser, computableWorker, ParameterVectorGradientUpdatable.class, generateDebugConfigurationObject() );

    FutureTask<Integer> worker = new FutureTask<Integer>(workerService);

    computableWorkers.add(computableWorker);
    workerServices.add(workerService);
    workers.add(worker);

    pool.submit(worker);
  }

  @Test
  public void testWorkerService() throws Exception {
    workers.get(0).get();
    workers.get(1).get();
    workers.get(2).get();
    master.get();
/*
    // Bozo numbers
    assertEquals(Integer.valueOf(12100), computableWorkers.get(0).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(1).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(2).getResults().get());
    assertEquals(Integer.valueOf(51570), computableMaster.getResults().get());
*/
   }
}