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

package com.cloudera.knittingboar.metrics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.utils.DataUtils;
import com.cloudera.knittingboar.utils.DatasetConverter;
import com.cloudera.knittingboar.utils.Utils;

import junit.framework.TestCase;

/**
 * Test applying a model to the test data in the 20newsgroups dataset
 * 
 * @author jpatterson
 *
 */
public class Test20NewsApplyModel extends TestCase {

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
  
  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTestDirInput = "" + unzipDir.toString() + "/KBoar-test/";
    
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 500);
    
    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY);

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
  
  
  public void testLoad20NewsModel() throws Exception {
    
    
    File file20News = DataUtils.getTwentyNewsGroupDir();
    
    DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-test/", strKBoarTestDirInput, 12000);
    
    POLRModelTester tester = new POLRModelTester();
    
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    tester.setConf(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars
    try {
      tester.LoadConfigVarsLocally();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( "Conf load fail: shutting down." );
      assertEquals( 0, 1 );
    }
    // now construct any needed machine learning data structures based on config
    tester.Setup();
    tester.Load( "src/test/resources/KBoar_Sample.model" ); //"/tmp/TestRunPOLRMasterAndNWorkers.20news.model" ); //  model20News.toString() );
    
    // ------------------    
    
    Path testData20News = new Path(strKBoarTestDirInput);
 
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(testData20News, job);
    
    System.out.println( "split count: " + splits.length );
        
      InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[0]);
      tester.setupInputSplit(custom_reader_0);
      
      tester.RunThroughTestRecords();
    
  }

    
}
