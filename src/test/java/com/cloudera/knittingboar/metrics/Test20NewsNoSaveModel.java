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

import java.io.IOException;
import java.util.ArrayList;

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
import com.cloudera.knittingboar.sgd.POLRMasterDriver;
import com.cloudera.knittingboar.sgd.POLRWorkerDriver;

import junit.framework.TestCase;

public class Test20NewsNoSaveModel extends TestCase {
  
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
  
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/"));  
    
  private static Path testData20News = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/test/"));  
  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");

    return c;
    
  }  
  
  public InputSplit[] generateDebugSplits( Path input_path, JobConf job ) {
        
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");

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
  
  public void testRunMasterAndTwoWorkers() throws Exception {

    int num_passes = 15;
    
    POLRMasterDriver master = new POLRMasterDriver();
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.setConf(this.generateDebugConfigurationObject());
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
 
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    // TODO: work on this, splits are generating for everything in dir
    InputSplit[] splits = generateDebugSplits(workDir, job);
      
    System.out.println( "split count: " + splits.length );
    
    ArrayList<POLRWorkerDriver> workers = new ArrayList<POLRWorkerDriver>();
    
    for ( int x = 0; x < splits.length; x++ ) {
      
      POLRWorkerDriver worker_model_builder = new POLRWorkerDriver(); //workers.get(x);
      worker_model_builder.internalID = String.valueOf(x);
      // simulates the conf stuff
      worker_model_builder.setConf(this.generateDebugConfigurationObject());
        
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
        
        // TODO: save model to HDFS
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
        
        master.SaveModelLocally("/tmp/TestRunPOLRMasterAndNWorkers.20news.model");
        
      } // if        

    } // for
    
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
    
    tester.SetCore(master.polr, master.polr_modelparams, workers.get(0).getRecordFactory());
    
    //fullRCV1Dir
    InputSplit[] splits_test_data = generateDebugSplits(testData20News, job);
    
    System.out.println( "split count: " + splits_test_data.length );

    InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits_test_data[0]);

    // TODO: set this up to run through the conf pathways
    tester.setupInputSplit(custom_reader_0);
    
    tester.RunThroughTestRecords();    
    
  }  
  
}
