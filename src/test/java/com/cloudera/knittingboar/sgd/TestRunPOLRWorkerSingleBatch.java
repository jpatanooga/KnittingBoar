package com.cloudera.wovenwabbit.sgd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;

import com.cloudera.wovenwabbit.io.InputRecordsSplit;
import com.cloudera.wovenwabbit.messages.GradientUpdateMessage;

import junit.framework.TestCase;

public class TestRunPOLRWorkerSingleBatch extends TestCase {

  
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
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );
    
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
  
  public InputSplit[] generateDebugSplits( String input_file, JobConf job ) {
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    // ---- set where we'll read the input files from -------------
    FileInputFormat.setInputPaths(job, workDir);


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
  
  
  public void testRunSingleBatch() throws Exception {

    POLRWorkerDriver worker_model_builder_0 = new POLRWorkerDriver();
    
    // simulates the conf stuff
    worker_model_builder_0.debug_setConf(this.generateDebugConfigurationObject());
  
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits("kboar-shard-0.txt", job);
    InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
      
      // TODO: set this up to run through the conf pathways
    worker_model_builder_0.setupInputSplit(custom_reader);
    worker_model_builder_0.LoadConfigVarsLocally();
    worker_model_builder_0.Setup();
    
    System.out.println( "> Feature Size: " + worker_model_builder_0.FeatureVectorSize );
    System.out.println( "> Category Size: " + worker_model_builder_0.num_categories );
    
    worker_model_builder_0.RunNextTrainingBatch();
    
    GradientUpdateMessage msg = worker_model_builder_0.GenerateUpdateMessage();
    
    msg.gradient.Debug();
    
  }
  
  
}
