package com.cloudera.knittingboar.sgd.iterativereduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
//import com.cloudera.knittingboar.yarn.AvroUtils;
import com.cloudera.iterativereduce.Utils;
import com.cloudera.iterativereduce.yarn.appmaster.ApplicationMasterService;
import com.cloudera.iterativereduce.ComputableMaster;
import com.cloudera.iterativereduce.yarn.appworker.ApplicationWorkerService;
import com.cloudera.iterativereduce.ComputableWorker;
import com.cloudera.iterativereduce.io.HDFSLineParser;
import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.iterativereduce.yarn.avro.generated.FileSplit;
import com.cloudera.iterativereduce.yarn.avro.generated.StartupConfiguration;
import com.cloudera.iterativereduce.yarn.avro.generated.WorkerId;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TestPOLRIterativeReduce {

  InetSocketAddress masterAddress;
  ExecutorService pool;

  private ApplicationMasterService<ParameterVectorGradientUpdatable> masterService;
  private Future<Integer> master;
  private ComputableMaster<ParameterVectorGradientUpdatable> computableMaster;

  private ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable>> workerServices = new ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable>>();
  private ArrayList<Future<Integer>> workers = new ArrayList<Future<Integer>>();
  private ArrayList<ComputableWorker<ParameterVectorGradientUpdatable>> computableWorkers = new ArrayList<ComputableWorker<ParameterVectorGradientUpdatable>>();

  
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    
    //Logger.getRootLogger().setLevel(Level.TRACE);
    //LogFactory.getFactory()
    //Logger foo = LogFactory
     Logger logger = Logger.getLogger(ApplicationWorkerService.class);
    logger.setLevel(Level.ERROR);
    
    logger = Logger.getLogger(ApplicationMasterService.class);
    logger.setLevel(Level.ERROR);
        
    // org.apache.avro.ipc.NettyTransceiver
    Logger.getLogger("org.apache.avro.ipc.NettyTransceiver").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.avro.ipc.NettyServer").setLevel(Level.ERROR);
    //com.cloudera.knittingboar.yarn.appworker.HDFSLineParser
    //Logger.getLogger("com.cloudera.knittingboar.yarn.appworker.HDFSLineParser").setLevel(Level.FATAL);
   
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

  /**
   * TODO: give the system multiple files and create the right number of splits
   * 
   * TODO: StartupConfiguration needs to be fed from the Configuration object somehow
   * 
   * TODO: what event do I have for when all the work is done?
   * - maybe a "completion()" method in ComputableMaster ?
   * 
   * @throws Exception
   */
  public void setUpMaster() throws Exception {
    
    System.out.println( "start-ms:" + System.currentTimeMillis() );
    
    // /Users/jpatterson/Downloads/datasets/20news-kboar/train3/kboar-shard-0.txt
    
//    FileSplit split = FileSplit.newBuilder()
//        .setPath("/Users/jpatterson/Downloads/datasets/20news-kboar/train3/kboar-shard-0.txt").setOffset(0).setLength(8348890)
//        .build();

    FileSplit split = FileSplit.newBuilder()
    .setPath("/Users/jpatterson/Downloads/datasets/20news-kboar/train3/kboar-shard-0.txt").setOffset(0).setLength(8348890)
    .build();    
    
    // hey MK, how do I set multiple splits or splits of multiple files?
    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(200).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);
    workers.put(Utils.createWorkerId("worker2"), conf);
    workers.put(Utils.createWorkerId("worker3"), conf);

    //computableMaster = new CompoundAdditionMaster();
    computableMaster = new POLRMasterNode();
    masterService = new ApplicationMasterService<ParameterVectorGradientUpdatable>(masterAddress,
        workers, computableMaster, ParameterVectorGradientUpdatable.class, null, generateDebugConfigurationObject() );

    master = pool.submit(masterService);
  }

  private void setUpWorker(String name) {
    //HDFSLineParser parser = new HDFSLineParser();
    
    TextRecordParser parser = new TextRecordParser();
    
    ComputableWorker<ParameterVectorGradientUpdatable> computableWorker = new POLRWorkerNode();
    final ApplicationWorkerService<ParameterVectorGradientUpdatable> workerService = new ApplicationWorkerService<ParameterVectorGradientUpdatable>(
        name, masterAddress, parser, computableWorker, ParameterVectorGradientUpdatable.class, generateDebugConfigurationObject() );

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
/*
    // Bozo numbers
    assertEquals(Integer.valueOf(12100), computableWorkers.get(0).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(1).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(2).getResults().get());
    assertEquals(Integer.valueOf(51570), computableMaster.getResults().get());
*/
   }
}