package com.cloudera.knittingboar.sgd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.utils.Utils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;


public class TestWorkerAndMaster extends TestCase {


  public static BufferedReader open(String inputFile) throws IOException {
    InputStream in;
    try {
      in = Resources.getResource(inputFile).openStream();
    } catch (IllegalArgumentException e) {
      in = new FileInputStream(new File(inputFile));
    }
    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }
  
  
  
  
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
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Documents/workspace/WovenWabbit/data/donut_no_header.csv"));  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 2);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 10);
    
    
    
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.CSV_RECORDFACTORY);
    
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );
    
    // predictor label names
    c.set( "com.cloudera.knittingboar.setup.PredictorLabelNames", "x,y" );

    // predictor var types
    c.set( "com.cloudera.knittingboar.setup.PredictorVariableTypes", "numeric,numeric" );
    
    // target variables
    c.set( "com.cloudera.knittingboar.setup.TargetVariableName", "color" );

    // column header names
    c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
    //c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "\"x\",\"y\",\"shape\",\"color\",\"k\",\"k0\",\"xx\",\"xy\",\"yy\",\"a\",\"b\",\"c\",\"bias\"\n" );
    
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
  /**
   * 1. Setup Worker
   * 
   * 2. Generate some gradient
   * 
   * 3. Construct a gradient update message
   * 
   * 4. simulate generation of a PVec update message
   * 
   * 5. update the local POLR driver w the PVec message
   * 
   * 6. check the local gradient and pvec matrices
   * @throws Exception 
   * 
   */  
  public void testBasicMessageFlowBetweenMasterAndWorker() throws Exception {
    
    // 1. Setup Worker ---------------------------------------------
    
    System.out.println( "\n------ testBasicMessageFlowBetweenMasterAndWorker --------- ");
 

    
    POLRMasterDriver master = new POLRMasterDriver();
    //master.LoadConfig();
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
    
    
    
    POLRWorkerDriver worker_model_builder = new POLRWorkerDriver();
    
    // generate the debug conf ---- normally setup by YARN stuff
    worker_model_builder.debug_setConf(this.generateDebugConfigurationObject());
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(workDir, job);

    InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
      
      // TODO: set this up to run through the conf pathways
    worker_model_builder.setupInputSplit(custom_reader);
    
    worker_model_builder.LoadConfigVarsLocally();

    worker_model_builder.Setup();    
    
    
    for ( int x = 0; x < 25; x++) {
      
      worker_model_builder.RunNextTrainingBatch();
      
      System.out.println( "---------- cycle " + x + " done ------------- " );

  } // for    
    worker_model_builder.polr.Debug_PrintGamma();
   
    // 3. generate a gradient update message ---------------------------------------------
    

    GradientUpdateMessage msg = worker_model_builder.GenerateUpdateMessage();
    
    //msg.gradient.Debug();
    
    
    master.AddIncomingGradientMessageToQueue(msg);
    
    master.RecvGradientMessage(); // process msg
    
    // 5. pass global pvector update message back to worker process, update driver pvector
    
    
    master.GenerateGlobalUpdateVector();
    
    GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();
    //returned_msg.parameter_vector.set(0, 0, -1.0);
    
    //Utils.PrintVector(returned_msg.parameter_vector.viewRow(0));
    
    worker_model_builder.ProcessIncomingParameterVectorMessage(returned_msg);
    //returned_msg.parameter_vector.set(0, 0, -1.0);
    System.out.println( "---------- " );
    Utils.PrintVector(returned_msg.parameter_vector.viewRow(0));
    
//    System.out.println( "Master Param Vector: " + returned_msg.parameter_vector.get(0, 0) + ", " + returned_msg.parameter_vector.get(0, 1) );
    
//    assertEquals( -1.0, returned_msg.parameter_vector.get(0, 0) );
//    assertEquals( 1.0, returned_msg.parameter_vector.get(0, 1) );    
  
    
    //worker.ProcessIncomingParameterVectorMessage(returned_msg);
    
    worker_model_builder.polr.Debug_PrintGamma();
    
    
  }
  
  
  /**
   * Runs 10 passes of 2 subsets of the donut data
   * - between each pass, the parameter vector is updated
   * - at the end, we compare to OLR
   * @throws Exception 
   * 
   */
  public void testOLRvs10PassesOfPOLR() throws Exception {
    

    // config ------------
    
    System.out.println( "\n------ testOLRvs10PassesOfPOLR --------- ");

 
    
    
    POLRWorkerDriver olr_run = new POLRWorkerDriver();
    
    // generate the debug conf ---- normally setup by YARN stuff
    olr_run.debug_setConf(this.generateDebugConfigurationObject());
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(workDir, job);

    InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
      
      // TODO: set this up to run through the conf pathways
    olr_run.setupInputSplit(custom_reader);
    
    olr_run.LoadConfigVarsLocally();

    olr_run.Setup();    
    
    
    for ( int x = 0; x < 25; x++) {
      
      olr_run.RunNextTrainingBatch();
      
      System.out.println( "---------- cycle " + x + " done ------------- " );

  } // for    
    
    
    
    
    /*
     * 
     * 
     * 
     * 
     * ----------------------- now run the parallel version -----------------
     * 
     * 
     * 
     * 
     * 
     * 
     * 
     */
/*    
    System.out.println( "\n\n------- POLR: Start ---------------" );
    
    POLRMasterDriver master = new POLRMasterDriver();
    //master.LoadConfig();
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
    
    
    
    
    //LogisticModelBuilder model_builder = new LogisticModelBuilder();
    POLRWorkerDriver worker = new POLRWorkerDriver();
    
 
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    worker.debug_setConf(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars
    try {
      worker.LoadConfigVarsLocally();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( "Conf load fail: shutting down." );
      assertEquals( 0, 1 );
    }
    // now construct any needed machine learning data structures based on config
    worker.Setup();
    worker.DebugPrintConfig();
    // ------------------
    
    
    // 2. Train a batch, generate some gradient ---------------------------------------------
    
    for (int pass = 0; pass < passes; pass++) {

      
      // --- run a pass -------------------------
      BufferedReader in = open(inputFile);
      
      String line = in.readLine();
      
      line = in.readLine(); // skip first line IF this is a CSV
      
      while (line != null) {
         
        worker.IncrementallyTrainModelWithRecord( line );
        
        line = in.readLine();
        
      } // while
      
      in.close();
      // ------- end of the pass -------------------

      // -------------- simulate message passing --------------
      GradientUpdateMessage msg0 = worker.GenerateUpdateMessage();
      master.AddIncomingGradientMessageToQueue(msg0);
      master.RecvGradientMessage(); // process msg
      
      GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();

      worker.ProcessIncomingParameterVectorMessage(returned_msg);
      
      System.out.println( "POLR: Updating Worker Parameter Vector" );
      
    } // for
   
    System.out.println( "POLR: Debug Beta / Gamma" );
    worker.polr.Debug_PrintGamma();
   
    // 3. generate a gradient update message ---------------------------------------------
        
    
    */
    
  }
  
}
