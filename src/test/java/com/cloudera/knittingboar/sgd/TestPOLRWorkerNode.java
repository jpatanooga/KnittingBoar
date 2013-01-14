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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import junit.framework.TestCase;

import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRWorkerNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;

/**
 * testing basic mechanics of the worker nodes in POLR
 * 
 * 
 * 
 * @author jpatterson
 *
 */
public class TestPOLRWorkerNode extends TestCase {

  
  
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
  
  private static int feature_vector_size = 10;
  private static Path workDir = new Path( "src/test/resources/donut_no_header.csv" );  
  /*
  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTestDirInput = "" + unzipDir.toString() + "/KBoar-test/";
*/    

  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 2);
    
    
    
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.CSV_RECORDFACTORY);
    
    
    // local input split path
//    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );
    
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
  
  
  
  public void testConfiguration() {
    
    POLRWorkerNode worker = new POLRWorkerNode();

    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    worker.setup(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars

    // test the base conf stuff ------------
    
    assertEquals( worker.getConf().getInt("com.cloudera.knittingboar.setup.FeatureVectorSize", 0), 10 );
//    assertEquals( worker.getConf().get("com.cloudera.knittingboar.setup.LocalInputSplitPath"), "hdfs://127.0.0.1/input/0" );
    assertEquals( worker.getConf().get("com.cloudera.knittingboar.setup.PredictorLabelNames"), "x,y" );
    assertEquals( worker.getConf().get("com.cloudera.knittingboar.setup.PredictorVariableTypes"), "numeric,numeric" );
    assertEquals( worker.getConf().get("com.cloudera.knittingboar.setup.TargetVariableName"), "color" );
    assertEquals( worker.getConf().get("com.cloudera.knittingboar.setup.ColumnHeaderNames"), "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
  
    // now test the parsed stuff ------------
  
    //worker.csvVectorFactory
  
  
  }
  
  
  
  
  
  /**
   * [ ******* Rebuilding this currently ******* ]
   * 
   * Tests replacing the beta, presumably from the master, after we've run POLR a bit 
   * @throws Exception 
   */
  public void testReplaceBetaMechanics() throws Exception {
    
    System.out.println( "\n------ testReplaceBetaMechanics --------- ");
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);
    
    InputSplit[] splits = generateDebugSplits(workDir, job);
    
    System.out.println( "split count: " + splits.length );
    
    POLRWorkerNode worker_model_builder = new POLRWorkerNode();
    
 
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    worker_model_builder.setup(this.generateDebugConfigurationObject());
    
    System.out.println("split: " + splits[0].toString());
     
    TextRecordParser txt_reader = new TextRecordParser();

    long len = Integer.parseInt(splits[0].toString().split(":")[2]
        .split("\\+")[1]);

    txt_reader.setFile(splits[0].toString().split(":")[1], 0, len);

    worker_model_builder.setRecordParser(txt_reader);

    
   
    
//      worker_model_builder.RunNextTrainingBatch();
    worker_model_builder.compute();
    
//    worker_model_builder.polr.Set
    
    // ------------------- now replace beta ------------
    
    double val1 = -1.0;
    
    // GradientBuffer g0 = new GradientBuffer( 2, worker_model_builder.FeatureVectorSize );
    Matrix m = new DenseMatrix( 2, feature_vector_size );

     for ( int x = 0; x < feature_vector_size; x++ ) {
       
       m.set(0, x, val1);
       
     }
  
     worker_model_builder.polr.SetBeta(m);
     
     for ( int x = 0; x < feature_vector_size; x++ ) {
     
       assertEquals( worker_model_builder.polr.noReallyGetBeta().get(0, x), val1 );
       
     }
 
     System.out.println( "--------------------------------\n" );
   
  }

    
  // ---------- older tests -------------
  
  public static BufferedReader open(String inputFile) throws IOException {
    InputStream in;
    try {
      in = Resources.getResource(inputFile).openStream();
    } catch (IllegalArgumentException e) {
      in = new FileInputStream(new File(inputFile));
    }
    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }
  
  
  
  /**
   * [ ******* Rebuilding this currently ******* ]
   * @throws Exception 
   */
  public void testPOLROnFullDatasetRun() throws Exception {
    
    POLRWorkerNode worker_model_builder = new POLRWorkerNode();
    
    // generate the debug conf ---- normally setup by YARN stuff
    worker_model_builder.setup(this.generateDebugConfigurationObject());
    
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(workDir, job);

//    InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[0]);
      
      // TODO: set this up to run through the conf pathways
//    worker_model_builder.setupInputSplit(custom_reader);
/*    
    worker_model_builder.LoadConfigVarsLocally();

    worker_model_builder.Setup();    
  */  

    
    TextRecordParser txt_reader = new TextRecordParser();

    long len = Integer.parseInt(splits[0].toString().split(":")[2]
        .split("\\+")[1]);

    txt_reader.setFile(splits[0].toString().split(":")[1], 0, len);

    worker_model_builder.setRecordParser(txt_reader);
    
    
    //for ( int x = 0; x < 5; x++) {
      
      worker_model_builder.compute();
      
      //System.out.println( "---------- cycle " + x + " done ------------- " );

  //} // for    
    
    
    // ------ move this loop into the POLR Worker Driver --------
    
        
    
    
   // worker_model_builder.PrintModelStats();
    
    assertEquals(1.0e-4, worker_model_builder.polr_modelparams.getLambda(), 1.0e-9);
    assertEquals(10, worker_model_builder.polr_modelparams.getNumFeatures());
    assertTrue(worker_model_builder.polr_modelparams.useBias());
    assertEquals("color", worker_model_builder.polr_modelparams.getTargetVariable());
    
    System.out.println("done!");
    
    
    assertNotNull(0);
    
  }
  
  
  
}
