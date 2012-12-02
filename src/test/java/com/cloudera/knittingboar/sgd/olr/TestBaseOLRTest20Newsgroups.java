/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.knittingboar.sgd.olr;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.metrics.POLRMetrics;
import com.cloudera.knittingboar.metrics.POLRModelTester;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;

import junit.framework.TestCase;

/**
 * Mainly just a demo to show how we'd test the 20Newsgroups model generated
 * with OLR
 * 
 * @author jpatterson
 *
 */
public class TestBaseOLRTest20Newsgroups extends TestCase {

  private static Path testData20News = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/test/kboar-shard-0.txt"));  

  //private static Path model20News = new Path( "/Users/jpatterson/Downloads/datasets/20news-kboar/models/model_10_31pm.model" ); 
  private static Path model20News = new Path( "/tmp/olr-news-group.model" );
  
  //private static Path testData20News = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/test/"));  
  
  private static final int FEATURES = 10000;
  
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
  
  POLRMetrics metrics = new POLRMetrics();
  
  //double averageLL = 0.0;
  //double averageCorrect = 0.0;
  double averageLineCount = 0.0;
  int k = 0;
  double step = 0.0;
  int[] bumps = new int[]{1, 2, 5};
  double lineCount = 0;
  
  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY);

    return c;
    
  }  
  
  public InputSplit[] generateDebugSplits( Path input_path, JobConf job ) {
    
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    // ---- set where we'll read the input files from -------------
    //FileInputFormat.setInputPaths(job, workDir);
    FileInputFormat.setInputPaths(job, input_path);


      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      //LongWritable key = new LongWritable();
      //Text value = new Text();

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
  
  
  
  
 
  
  public void testResults() throws Exception {
    
    OnlineLogisticRegression classifier = ModelSerializer.readBinary(new FileInputStream(model20News.toString()), OnlineLogisticRegression.class);

    
    Text value = new Text();
    long batch_vec_factory_time = 0;
    int k = 0;
    int num_correct = 0;
    
    
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
//    InputSplit[] splits = generateDebugSplits(inputDir, job);
  
    //fullRCV1Dir
    InputSplit[] splits = generateDebugSplits(testData20News, job);
    
    System.out.println( "split count: " + splits.length );

        
        
      InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[0]);    
    
      TwentyNewsgroupsRecordFactory VectorFactory = new TwentyNewsgroupsRecordFactory("\t");
    
    
    for (int x = 0; x < 8000; x++ ) {
      
      if ( custom_reader_0.next(value)) {
        
        long startTime = System.currentTimeMillis();

        Vector v = new RandomAccessSparseVector(FEATURES);
        int actual = VectorFactory.processLine(value.toString(), v);

        long endTime = System.currentTimeMillis();

        //System.out.println("That took " + (endTime - startTime) + " milliseconds");
        batch_vec_factory_time += (endTime - startTime);
        
        
        String ng = VectorFactory.GetClassnameByID(actual); //.GetNewsgroupNameByID( actual );
        
        // calc stats ---------
        
        double mu = Math.min(k + 1, 200);
        double ll = classifier.logLikelihood(actual, v);  
        //averageLL = averageLL + (ll - averageLL) / mu;
        metrics.AvgLogLikelihood = metrics.AvgLogLikelihood + (ll - metrics.AvgLogLikelihood) / mu; 

        Vector p = new DenseVector(20);
        classifier.classifyFull(p, v);
        int estimated = p.maxValueIndex();

        int correct = (estimated == actual? 1 : 0);
        if (estimated == actual) {
          num_correct++;
        }
        //averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
        metrics.AvgCorrect = metrics.AvgCorrect + (correct - metrics.AvgCorrect) / mu; 
        
        //this.polr.train(actual, v);
        
        
        k++;
//        if (x == this.BatchSize - 1) {
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
        
        if (k % (bump * scale) == 0) {
          step += 0.25;
          
          System.out.printf("Worker %s:\t Tested Recs: %10d, numCorrect: %d, AvgLL: %10.3f, Percent Correct: %10.2f, VF: %d\n",
              "OLR-standard-test", k, num_correct, metrics.AvgLogLikelihood, metrics.AvgCorrect * 100, batch_vec_factory_time);
          
        }
        
        classifier.close();                  
      
      }  else {
        
        // nothing else to process in split!
        break;
        
      } // if
      
      
    } // for the number of passes in the run    
    
    
    
  }
  
  
}
