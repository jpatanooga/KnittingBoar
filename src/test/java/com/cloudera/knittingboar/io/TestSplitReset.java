package com.cloudera.knittingboar.io;

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

import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRWorkerNode;

import junit.framework.TestCase;


public class TestSplitReset extends TestCase {
  
  
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
  
//  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/"));  

  // /Users/jpatterson/Downloads/datasets/20news-kboar/models
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/models/input/"));  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 500);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 2);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");

    return c;
    
  }  
  
  public InputSplit[] generateDebugSplits( Path input_path, JobConf job ) {
    
    long block_size = localFs.getDefaultBlockSize();
//    localFs.
    
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
  
  public void testReset() throws IOException {
    
    //TextRecordParser lineParser = null;
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(workDir, job);
      
    System.out.println( "split count: " + splits.length );

    //ArrayList<POLRWorkerNode> workers = new ArrayList<POLRWorkerNode>();
  
    //ArrayList<ParameterVectorGradientUpdatable> master_results = new ArrayList<ParameterVectorGradientUpdatable>();
    
    //for ( int x = 0; x < splits.length; x++ ) {
      
      //POLRWorkerNode worker_model_builder = new POLRWorkerNode(); //workers.get(x);
      //worker_model_builder.internalID = String.valueOf(x);
      // simulates the conf stuff
      //worker_model_builder.setup(this.generateDebugConfigurationObject());
        
        
      //InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[x]);
      TextRecordParser txt_reader = new TextRecordParser();
      
      long len = Integer.parseInt( splits[0].toString().split(":")[2].split("\\+")[1] );

      txt_reader.setFile(splits[0].toString().split(":")[1], 0, len);    
      
      Text csv_line = new Text();
      int x = 0;
      while (txt_reader.hasMoreRecords()) {
        
        
        txt_reader.next(csv_line);
        x++;
      }
      
      System.out.println( "read recs: " + x );
      
      txt_reader.reset();
      //txt_reader.setFile(splits[0].toString().split(":")[1], 0, len);   
      x = 0;
      while (txt_reader.hasMoreRecords()) {
        
        
        txt_reader.next(csv_line);
        x++;
      }
      
      System.out.println( "[after reset] read recs: " + x );
    
  }

}
