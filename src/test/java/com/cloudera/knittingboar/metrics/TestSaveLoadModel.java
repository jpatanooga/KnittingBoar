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
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.iterativereduce.irunit.IRUnitDriver;
import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRMasterNode;
import com.cloudera.knittingboar.utils.DataUtils;
import com.cloudera.knittingboar.utils.DatasetConverter;

import junit.framework.TestCase;

/**
 * This unit test tests running a 4 worker simulated parallel SGD process, saving the model,
 * loading the model, and then checking to see that the parameters of the model deserialized correctly
 * 
 * @author jpatterson
 *
 */
public class TestSaveLoadModel extends TestCase {
  
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
  
  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "TestSaveLoadModel");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTrainDirInput = "" + unzipDir.toString() + "/KBoar-train/";
  //private static String strKBoarTestDirInput = "" + unzipDir.toString() + "/KBoar-test/";
  

  // location of N splits of KBoar converted data ---
  private static Path workDir = new Path( strKBoarTrainDirInput ); //DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/" );
    
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");

    return c;
    
  }  

  
  public void testRunMasterAndFourWorkers() throws Exception {

    DataUtils.getTwentyNewsGroupDir();   
    
    // convert the training data into 4 shards
    DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarTrainDirInput, 3000);
    
    // convert the test data into 1 shard
//    DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-test/", strKBoarTestDirInput, 12000);
    
    
    int num_passes = 15;
    
    
    String[] props = {
        "app.iteration.count",
    "com.cloudera.knittingboar.setup.FeatureVectorSize",
    "com.cloudera.knittingboar.setup.numCategories",
    "com.cloudera.knittingboar.setup.RecordFactoryClassname"
    };
    
    IRUnitDriver polr_driver = new IRUnitDriver("src/test/resources/app_unit_test.properties", props );
    
    polr_driver.SetProperty("app.input.path", strKBoarTrainDirInput);
    
    polr_driver.Setup();
    polr_driver.SimulateRun();
    
    System.out.println("\n\nComplete...");

    POLRMasterNode IR_Master = (POLRMasterNode)polr_driver.getMaster();
    
    Path out = new Path("/tmp/TestSaveLoadModel.model");
    FileSystem fs = out.getFileSystem(defaultConf);
    FSDataOutputStream fos = fs.create(out);

    IR_Master.complete(fos);

    fos.flush();
    fos.close();        
    
    System.out.println("\n\nModel Saved: /tmp/TestSaveLoadModel.model" );
    
    
    System.out.println( "\n\n> Loading Model for tests..." );
    
    
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
    tester.Load( "/tmp/TestSaveLoadModel.model" );
    
    assertEquals( 1.0e-4, tester.polr.getLambda() );
    
    assertEquals( 20, tester.polr.numCategories() );
    
  }  

  
  
}
