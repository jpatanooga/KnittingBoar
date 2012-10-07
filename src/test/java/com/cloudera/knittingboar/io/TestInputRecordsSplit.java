package com.cloudera.wovenwabbit.io;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import junit.framework.TestCase;

public class TestInputRecordsSplit  extends TestCase {


  private static final Log LOG = LogFactory.getLog(TestInputRecordsSplit.class.getName());

  private static int MAX_LENGTH = 1000;
  
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
  
  private static Path workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")), "TestInputRecordsSplit").makeQualified(localFs);  
  
  
  /**
   * create an InputRecordSplit and then read some records
   * 
   * - make sure we maintain split discipline
   * @throws IOException 
   * 
   */
  public void testReadSplitViaInputRecordsSplit() throws IOException {
    
    // InputRecordsSplit(JobConf jobConf, InputSplit split)
    
    // needs to get a jobConf from somewhere, under the hood
    
    // needs a split calculated from the aforementioned jobConf
    
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(workDir, "testReadSplitViaInputRecordsSplit.txt");

    int tmp_file_size = 2000;
    
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
    
    System.out.println( "file write complete, wrote " + tmp_file_size + " recs" );
    
    
    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
 
    System.out.println( "> setting splits for: " + workDir );
    
//    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, file);


      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();

      int numSplits = 1;
      
      InputSplit[] splits = format.getSplits(job, numSplits);
      
      LOG.info("requested " + numSplits + " splits, splitting: got =        " + splits.length);
      
      System.out.println( "---- debug splits --------- " );
      
      //InputSplit test_split = null;
      
      int total_read = 0;
      
      for (int x = 0; x < splits.length; x++) {
        
        System.out.println( "> Split [" + x + "]: " + splits[x].getLength()  );
        
        int count = 0;
        InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[x]);
        while ( custom_reader.next(value)) {
          
          count++;
          //
          
        }
        
        System.out.println( "read: " + count + " records for split " + x );
 
        total_read += count;
        
      } // for each split
      
      System.out.println( "--------- total read across all splits: " + total_read );
      
      assertEquals( tmp_file_size, total_read);
      
  }
  
  public void testRCV1Splits() throws IOException {
    
    String file_rcv1 = "/Users/jpatterson/Downloads/rcv1/rcv1.train.vw";
    
    System.out.println( "testRCV1Splits >> " + file_rcv1 );
    
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(file_rcv1);
    
    
    
    FileInputFormat.setInputPaths(job, file);


    // try splitting the file in a variety of sizes
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    LongWritable key = new LongWritable();
    Text value = new Text();

    int numSplits = 1;
    
    InputSplit[] splits = format.getSplits(job, numSplits);
    
    LOG.info("requested " + numSplits + " splits, splitting: got =        " + splits.length);
    
    System.out.println( "---- debug splits --------- " );
    
    //InputSplit test_split = null;
    
    int total_read = 0;
    
    for (int x = 0; x < splits.length; x++) {
      
      System.out.println( "> Split [" + x + "]: " + splits[x].toString() + ", len:" + splits[x].getLength()  );
      
      int count = 0;
      InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[x]);
      while ( custom_reader.next(value)) {
        
        count++;
        //
        
      }
      
      System.out.println( "read: " + count + " records for split " + x );

      total_read += count;
      
    } // for each split
    
    System.out.println( "total read across all splits: " + total_read );
        
    
  }
  
  
  
  
  
  
  public void testReadSplitViaInputRecordsSplit_SplitReset() throws IOException {
    
    // InputRecordsSplit(JobConf jobConf, InputSplit split)
    
    // needs to get a jobConf from somewhere, under the hood
    
    // needs a split calculated from the aforementioned jobConf
    
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(workDir, "testReadSplitViaInputRecordsSplit_SplitReset");

    int tmp_file_size = 2000;
    
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
    
    System.out.println( "file write complete, wrote " + tmp_file_size + " recs" );
    
    
    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
 
//    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, file);


      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();

      int numSplits = 1;
      
      InputSplit[] splits = format.getSplits(job, numSplits);
      
      LOG.info("requested " + numSplits + " splits, splitting: got =        " + splits.length);
      
      System.out.println( "---- testReadSplitViaInputRecordsSplit_SplitReset: debug splits --------- " );
      
      int total_read = 0;
      
        System.out.println( "> Split [0]: " + splits[0].getLength()  );
        
        int count = 0;
        InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
        while ( custom_reader.next(value)) {
          
          count++;
          
        }
        
        System.out.println( "read: " + count + " records for split " + 0 );

        int count_reset = 0;
        custom_reader.ResetToStartOfSplit();
        while ( custom_reader.next(value)) {
          
          count_reset++;
          
        }
        
        System.out.println( "read: " + count_reset + " records for split after reset " + 0 );
        
        assertEquals( count, count_reset );
        
      
  }
    
  
  
  
  
  
}