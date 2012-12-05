package com.cloudera.knittingboar.io;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
  

  private static Path workDir = new Path(System.getProperty("test.build.data", "/tmp/TestSplitReset/"));  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 2);
    

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
    
    
    Path file = new Path(workDir, "testGetSplits.txt");

    int tmp_file_size = 200000;
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    Writer writer = new OutputStreamWriter(localFs.create(file));
    try {
      for (int i = 0; i < tmp_file_size; i++) {
        writer.write("a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, 1, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, 99");
        writer.write("\n");
      }
    } finally {
      writer.close();
    }    
    
    System.out.println( "file write complete" );
    
    

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(workDir, job);
      
    System.out.println( "split count: " + splits.length );

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
