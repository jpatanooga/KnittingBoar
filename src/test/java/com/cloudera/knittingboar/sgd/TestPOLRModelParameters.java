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

package com.cloudera.knittingboar.sgd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.records.RecordFactory;

import junit.framework.TestCase;

public class TestPOLRModelParameters extends TestCase {
  
  
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
  
  //private static Path workDir = new Path(System.getProperty("/Users/jpatterson/Documents/workspace/WovenWabbit/data/donut_no_header.csv"));
  private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Documents/workspace/WovenWabbit/data/donut_no_header.csv"));  

  private static Path tmpDir = new Path("/tmp/");
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 20 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 2);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 10);
    
    
    
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.CSV_RECORDFACTORY);
    
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );
    
    // predictor label names
    c.set( "com.cloudera.knittingboar.setup.PredictorLabelNames", "x,y" );

    // predictor var types
    c.set( "com.cloudera.knittingboar.setup.PredictorVariableTypes", "numeric,numeric" );
    
    // target variables
    c.set( "com.cloudera.knittingboar.setup.TargetVariableName", "color" );

    // column header names
    c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
    //c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "\"x\",\"y\",\"shape\",\"color\",\"k\",\"k0\",\"xx\",\"xy\",\"yy\",\"a\",\"b\",\"c\",\"bias\"\n" );
    
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
  
  
  public void testSaveLoadForDonutRun() throws IOException, Exception {
    
    
    POLRWorkerDriver olr_run = new POLRWorkerDriver();
    
    // generate the debug conf ---- normally setup by YARN stuff
    olr_run.debug_setConf(this.generateDebugConfigurationObject());
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(workDir, job);

    InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
      
      // TODO: set this up to run through the conf pathways
    olr_run.setupInputSplit(custom_reader);
    
    olr_run.LoadConfigVarsLocally();

    olr_run.Setup();    
    
    
    for ( int x = 0; x < 25; x++) {
      
      olr_run.RunNextTrainingBatch();
      
      System.out.println( "---------- cycle " + x + " done ------------- " );

  } // for    
    
//    olr_run.PrintModelStats();
    
    
    
    //LogisticModelParameters lmp = model_builder.lmp;//TrainLogistic.getParameters();
    assertEquals(1.0e-4, olr_run.polr_modelparams.getLambda(), 1.0e-9);
    assertEquals(20, olr_run.polr_modelparams.getNumFeatures());
    assertTrue(olr_run.polr_modelparams.useBias());
    assertEquals("color", olr_run.polr_modelparams.getTargetVariable());    
    
    
  //localFs.delete(workDir, true);

    
    olr_run.SaveModelLocally("/tmp/polr_run.model");
    
    POLRWorkerDriver polr_new = new POLRWorkerDriver();
    
    
  }

}
