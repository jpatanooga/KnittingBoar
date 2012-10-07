package com.cloudera.knittingboar.sgd;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;

import junit.framework.TestCase;

/**
 * A simulation of a single POLR master and a worker for each generated split of the data
 * 
 * @author jpatterson
 *
 */
public class TestRunPOLRMasterAndNWorkers extends TestCase {
 
  
  
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
    
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 5);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");
    
/*    // predictor label names
    c.set( "com.cloudera.knittingboar.setup.PredictorLabelNames", "x,y" );

    // predictor var types
    c.set( "com.cloudera.knittingboar.setup.PredictorVariableTypes", "numeric,numeric" );
    
    // target variables
    c.set( "com.cloudera.knittingboar.setup.TargetVariableName", "color" );

    // column header names
    c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
    //c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "\"x\",\"y\",\"shape\",\"color\",\"k\",\"k0\",\"xx\",\"xy\",\"yy\",\"a\",\"b\",\"c\",\"bias\"\n" );
 */   
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
  
  
  public void testRunMasterAndTwoWorkers() throws Exception {
    
    POLRMasterDriver master = new POLRMasterDriver();
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.debug_setConf(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars
    try {
      master.LoadConfigVarsLocally();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( "Conf load fail: shutting down." );
      assertEquals( 0, 1 );
    }
    // now construct any needed machine learning data structures based on config
    master.Setup();
    // ------------------    
    
 
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(workDir, job);
      
    System.out.println( "split count: " + splits.length );

  
    
    ArrayList<POLRWorkerDriver> workers = new ArrayList<POLRWorkerDriver>();
    
    for ( int x = 0; x < splits.length; x++ ) {
      
      POLRWorkerDriver worker_model_builder = new POLRWorkerDriver(); //workers.get(x);
      worker_model_builder.internalID = String.valueOf(x);
      // simulates the conf stuff
      worker_model_builder.debug_setConf(this.generateDebugConfigurationObject());
        
        
      InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[x]);
        // TODO: set this up to run through the conf pathways
      worker_model_builder.setupInputSplit(custom_reader_0);
      
      worker_model_builder.LoadConfigVarsLocally();
      worker_model_builder.Setup();
      
      workers.add( worker_model_builder );
    
      System.out.println( "> Setup Worker " + x );
      
    }
    
    boolean bContinuePass = true;
    int x = 0;
    
    while (bContinuePass) {
        
      bContinuePass = false;
      
      for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
      
        //arContinueTracker[worker_id] 
        boolean result = workers.get(worker_id).RunNextTrainingBatch();
        if (result) {
          bContinuePass = true;
        }
        GradientUpdateMessage msg0 = workers.get(worker_id).GenerateUpdateMessage();
        
        master.AddIncomingGradientMessageToQueue(msg0);
        master.RecvGradientMessage(); // process msg
        
      }
      
 
        // TODO: save model to HDFS
      if (bContinuePass) {
        
        
        master.GenerateGlobalUpdateVector();
        
        GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();
  
        // process global updates
        for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
          
          workers.get(worker_id).ProcessIncomingParameterVectorMessage(returned_msg);

        }
        
        
        System.out.println( "---------- cycle " + x + " done in pass " + workers.get(0).GetCurrentLocalPassCount() + " ------------- " );

      } else {
        
        System.out.println( "---------- cycle " + x + " done in pass " + workers.get(0).GetCurrentLocalPassCount() + " ------------- " );
        
        System.out.println( "> Saving Model..." );
        
        master.SaveModelLocally("/tmp/TestRunPOLRMasterAndNWorkers.20news.model");
        
      } // if     
      
      x++;
      
    } // for
    
    
    
    
    
    
    
    
    
    
  }
  
  
}
