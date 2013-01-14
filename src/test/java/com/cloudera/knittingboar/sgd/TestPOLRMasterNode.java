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

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRMasterNode;

import junit.framework.TestCase;

/**
 * - Test messages coming in and merging into the master gradient buffer
 * 
 * - Test trigger message generation after gradient update
 * 
 * @author jpatterson
 *
 */
public class TestPOLRMasterNode extends TestCase {

  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10 );

    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.CSV_RECORDFACTORY);
    
    
    // predictor label names
    c.set( "com.cloudera.knittingboar.setup.PredictorLabelNames", "x,y" );

    // predictor var types
    c.set( "com.cloudera.knittingboar.setup.PredictorVariableTypes", "numeric,numeric" );
    
    // target variables
    c.set( "com.cloudera.knittingboar.setup.TargetVariableName", "color" );

    // column header names
    c.set( "com.cloudera.knittingboar.setup.ColumnHeaderNames", "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
    
    return c;
    
  }
  
  public void testMasterConfiguration() {
    
    POLRMasterNode master = new POLRMasterNode();

    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.setup(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars
/*    try {
      master.LoadConfigVarsLocally();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( "Conf load fail: shutting down." );
      assertEquals( 0, 1 );
    }
    // now construct any needed machine learning data structures based on config
    master.Setup();
*/
    // ------------------

    // test the base conf stuff ------------
    
    assertEquals( master.getConf().getInt("com.cloudera.knittingboar.setup.FeatureVectorSize", 0), 10 );
//    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.LocalInputSplitPath"), "hdfs://127.0.0.1/input/0" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.PredictorLabelNames"), "x,y" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.PredictorVariableTypes"), "numeric,numeric" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.TargetVariableName"), "color" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.ColumnHeaderNames"), "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
  
    // now test the parsed stuff ------------
  
    
  
  
  }
  
  
  
}
