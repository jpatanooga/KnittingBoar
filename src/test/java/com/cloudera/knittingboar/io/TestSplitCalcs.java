package com.cloudera.knittingboar.io;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.Random;

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

public class TestSplitCalcs extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestSplitCalcs.class.getName());

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
  
  private static Path workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")), "TestSplitCalcs").makeQualified(localFs);  
  
  
  
  /**
   * 
   * - use the TextInputFormat.getSplits() to test pulling split info
   * @throws IOException 
   * 
   */
  public void testGetSplits() throws IOException {
    
    TextInputFormat input = new TextInputFormat();
    
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
      
      assertEquals( 2, splits.length );
      
      System.out.println( "---- debug splits --------- " );
      
      for (int x = 0; x < splits.length; x++) {
        
        System.out.println( "> Split [" + x + "]: " + splits[x].getLength()  + ", " + splits[x].toString() + ", " + splits[x].getLocations()[0] );
        
        
        RecordReader<LongWritable, Text> reader = format.getRecordReader(splits[x], job, reporter);
        try {
          int count = 0;
          while (reader.next(key, value)) {
    
            if (count == 0) {
              System.out.println( "first: " + value.toString() );
              assertTrue( value.toString().contains("a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p") );
            }
          
            count++;
          }
          
          System.out.println( "last: " + value.toString() );
          
          assertTrue( value.toString().contains("a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p") );
          
        } finally {
          reader.close();
        }      

      } // for each split
      
      
  }
  
  
}
