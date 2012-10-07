package com.cloudera.wovenwabbit.records;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.wovenwabbit.io.InputRecordsSplit;
import com.cloudera.wovenwabbit.utils.Utils;

public class TestTwentyNewsgroupsCustomRecordParseOLRRun extends TestCase {

  
  
  private static final int FEATURES = 10000;

  private static final Log LOG = LogFactory.getLog(TestTwentyNewsgroupsCustomRecordParseOLRRun.class.getName());

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
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train3/"));  
    
  
  
  
  
  
  public void testRecordFactoryOnDatasetShard() throws Exception {

    
    // p.270 ----- metrics to track lucene's parsing mechanics, progress, performance of OLR ------------
    double averageLL = 0.0;
    double averageCorrect = 0.0;
    double averageLineCount = 0.0;
    int k = 0;
    double step = 0.0;
    int[] bumps = new int[]{1, 2, 5};
    double lineCount = 0;
    
    
    
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("\t");
    //rec_factory.setClassSplitString("\t");
    
    
    
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(workDir, "kboar-shard-0.txt");

    int tmp_file_size = 200000;
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");

    
    
    
    // matches the OLR setup on p.269 ---------------
    // stepOffset, decay, and alpha --- describe how the learning rate decreases
    // lambda: amount of regularization
    // learningRate: amount of initial learning rate
    OnlineLogisticRegression learningAlgorithm = 
        new OnlineLogisticRegression(
              20, FEATURES, new L1())
            .alpha(1).stepOffset(1000)
            .decayExponent(0.9) 
            .lambda(3.0e-5)
            .learningRate(20);    
    
    
    
    
    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
 
//    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);


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
      
      rec_factory.Debug();
      
      int total_read = 0;
      
      
      

      
      
      
      
      
      for (int x = 0; x < splits.length; x++) {
        
        System.out.println( "> Split [" + x + "]: " + splits[x].getLength()  );
        
        int count = 0;
        InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[x]);
        while ( custom_reader.next(value)) {
          
          
          Vector v = new RandomAccessSparseVector(TwentyNewsgroupsRecordFactory.FEATURES);
          int actual = rec_factory.processLine(value.toString(), v);
          
          String ng = rec_factory.GetNewsgroupNameByID(actual);
          
          // calc stats ---------
          
          double mu = Math.min(k + 1, 200);
          double ll = learningAlgorithm.logLikelihood(actual, v);  
          averageLL = averageLL + (ll - averageLL) / mu;

          Vector p = new DenseVector(20);
          learningAlgorithm.classifyFull(p, v);
          int estimated = p.maxValueIndex();

          int correct = (estimated == actual? 1 : 0);
          averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
          
          learningAlgorithm.train(actual, v);
          
          
          k++;
          
          int bump = bumps[(int) Math.floor(step) % bumps.length];
          int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
          
          if (k % (bump * scale) == 0) {
            step += 0.25;
            System.out.printf("%10d %10.3f %10.3f %10.2f %s %s\n",
                 k, ll, averageLL, averageCorrect * 100, ng,
                 rec_factory.GetNewsgroupNameByID(estimated));
          }
          
          learningAlgorithm.close();            
                 
          count++;
          
        }
        
        System.out.println( "read: " + count + " records for split " + x );
 
        total_read += count;
        
      } // for each split
      
      
      System.out.println( "total read across all splits: " + total_read );
      
      rec_factory.Debug();
      
    
  }
 
}
