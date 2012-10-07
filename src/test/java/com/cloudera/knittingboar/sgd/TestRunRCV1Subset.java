package com.cloudera.knittingboar.sgd;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;
import com.cloudera.knittingboar.records.RecordFactory;

import junit.framework.TestCase;

/**
 * A simulation of a single POLR master and a worker for each generated split of the data
 * for the RCV1 dataset:
 * 
 *    https://github.com/JohnLangford/vowpal_wabbit/wiki/Rcv1-example
 * 
 * @author jpatterson
 *
 */
public class TestRunRCV1Subset extends TestCase {
  
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
  
  private static Path inputDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/rcv1/subset/train/"));  
    
  private static Path fullRCV1Dir = new Path("/Users/jpatterson/Downloads/rcv1/rcv1.train.vw");
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 2);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.RCV1_RECORDFACTORY);
    
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
  
  
  public void testSplits() throws IOException {

    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(inputDir, job);
      
    System.out.println( "split count: " + splits.length );

    assertEquals( 10, splits.length );

    InputSplit[] splits_full = generateDebugSplits(fullRCV1Dir, job);
    
    System.out.println( "full rcv1 split count: " + splits_full.length );
  
  
    Text value = new Text();
    
    for ( int x = 0; x < splits_full.length; x++ ) {
        
      InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits_full[x]);

      custom_reader_0.next(value);
      System.out.println( x + " > " + value.toString() );
      
      custom_reader_0.next(value);
      System.out.println( x + " > " + value.toString() );

      custom_reader_0.next(value);
      System.out.println( x + " > " + value.toString() + "\n" );
      
    }  
  
  }
  
  
  public void testRunRCV1Subset() throws IOException, Exception {
    
   int num_passes = 15;
    
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
    InputSplit[] splits = generateDebugSplits(fullRCV1Dir, job);
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


    
    for ( int x = 0; x < num_passes; x++) {
        
      for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
      
        workers.get(worker_id).RunNextTrainingBatch();
        GradientUpdateMessage msg0 = workers.get(worker_id).GenerateUpdateMessage();
        
        master.AddIncomingGradientMessageToQueue(msg0);
        master.RecvGradientMessage(); // process msg
        
      }
        
      if (x < num_passes - 1) {
      
        master.GenerateGlobalUpdateVector();
        GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();
  
        // process global updates
        for ( int worker_id = 0; worker_id < workers.size(); worker_id++ ) {
          
          workers.get(worker_id).ProcessIncomingParameterVectorMessage(returned_msg);

        }
        
        
        System.out.println( "---------- cycle " + x + " done ------------- " );

      } else {
        
        System.out.println( "---------- cycle " + x + " done ------------- " );
        System.out.println( "> Saving Model..." );
        
        master.SaveModelLocally("/tmp/master_sgd.model");
        
      } // if
      
        
    } // for
    
    workers.get(0).Debug();
    
  }

}
