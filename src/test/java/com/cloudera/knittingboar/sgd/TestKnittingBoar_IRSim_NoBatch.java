package com.cloudera.knittingboar.sgd;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.iterativereduce.irunit.IRUnitDriver;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradient;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRMasterNode;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRWorkerNode;


public class TestKnittingBoar_IRSim_NoBatch  extends TestCase {

  
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
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/"));  
    
/*  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 500);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 3);
    
    c.setInt("com.cloudera.knittingboar.setup.LearningRate", 5);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");

    return c;
    
  }  
  
  public InputSplit[] generateDebugSplits( Path input_path, JobConf job ) {
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    
    // ---- set where we'll read the input files from -------------
    FileInputFormat.setInputPaths(job, input_path);


      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);

      int numSplits = 1;
      
      InputSplit[] splits = null;
      
      try {
        splits = format.getSplits(job, numSplits);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
       
      
      return splits;
    
    
  }
  
  
  public void testRunKBoar_IRSimulated_NWorkers() throws Exception {
    
    long ts_start = System.currentTimeMillis();
    
    System.out.println( "start-ms:" + ts_start );
    
    POLRMasterNode IR_Master = new POLRMasterNode();
    IR_Master.setup(this.generateDebugConfigurationObject());
    
 
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(workDir, job);
      
    System.out.println( "split count: " + splits.length );

    ArrayList<POLRWorkerNode> workers = new ArrayList<POLRWorkerNode>();
  
    ArrayList<ParameterVectorGradientUpdatable> master_results = new ArrayList<ParameterVectorGradientUpdatable>();
    
    for ( int x = 0; x < splits.length; x++ ) {
      
      POLRWorkerNode worker_model_builder = new POLRWorkerNode(); //workers.get(x);
      worker_model_builder.internalID = String.valueOf(x);
      // simulates the conf stuff
      worker_model_builder.setup(this.generateDebugConfigurationObject());
        
        
      //InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[x]);
      TextRecordParser txt_reader = new TextRecordParser();
      
      long len = Integer.parseInt( splits[x].toString().split(":")[2].split("\\+")[1] );

      txt_reader.setFile(splits[x].toString().split(":")[1], 0, len);
      
      // System.out.println( "> " + splits[x].toString().split(":")[1] );
      // System.out.println( "> " + splits[x].toString().split(":")[2] );
      
      worker_model_builder.setRecordParser(txt_reader);
      
      
      workers.add( worker_model_builder );
    
      System.out.println( "> Setup Worker " + x );
    } // for
    
    
    
  
    ArrayList<ParameterVectorGradientUpdatable> worker_results = new ArrayList<ParameterVectorGradientUpdatable>();
    boolean bContinuePass = true;
    int x = 0;
    
    //while (bContinuePass) {
    for (int iteration = 0; iteration < 3; iteration++ ) {
        
      bContinuePass = true;
      for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
      
        ParameterVectorGradientUpdatable result = workers.get(worker_id).compute();
        worker_results.add(result);
        ParameterVectorGradient msg0 = workers.get(worker_id).GenerateUpdate();
                
      } // for
      
      ParameterVectorGradientUpdatable master_result = IR_Master.compute(worker_results, master_results);
      System.out.println( "### Check iteration -----" ); 

      // process global updates
      for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
        
        workers.get(worker_id).update(master_result);
        
        workers.get(worker_id).IncrementIteration();

      }

        
       
    } // while
  
       
        System.out.println( "> Saving Model... " );
        
        Path out = new Path("/tmp/IR_Model_0.model");
        FileSystem fs = out.getFileSystem(defaultConf);
        FSDataOutputStream fos = fs.create(out);
    
        //LOG.info("Writing master results to " + out.toString());
        IR_Master.complete(fos);
    
        fos.flush();
        fos.close();        
        
        //break;
//        master.SaveModelLocally("/tmp/TestRunPOLRMasterAndNWorkers.20news.model");
        
      //} // if     
      
//      x++;
      
    
   
    
    
//    Utils.PrintVectorSection( master.global_parameter_vector.gamma.viewRow(0), 3 );
    
    
    long ts_total = System.currentTimeMillis() - ts_start;
    
    System.out.println( "total time in ms:" + ts_total );
    
    
    
  }
  */
  
  public void testIRUnit_POLR() {
    
    System.out.println( "Starting: testIRUnit_POLR" );
    
    String[] props = {
        "app.iteration.count",
    "com.cloudera.knittingboar.setup.FeatureVectorSize",
    "com.cloudera.knittingboar.setup.numCategories",
    "com.cloudera.knittingboar.setup.RecordFactoryClassname"
    };
    
    IRUnitDriver polr_driver = new IRUnitDriver("src/test/resources/app_unit_test.properties", props );
    
    polr_driver.Setup();
    polr_driver.SimulateRun();
    
    System.out.println("\n\nComplete...");
    
  }
    
  
}
