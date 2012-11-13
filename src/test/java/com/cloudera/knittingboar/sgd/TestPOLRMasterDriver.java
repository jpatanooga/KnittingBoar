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

import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;

import junit.framework.TestCase;

/**
 * - Test messages coming in and merging into the master gradient buffer
 * 
 * - Test trigger message generation after gradient update
 * 
 * @author jpatterson
 *
 */
public class TestPOLRMasterDriver extends TestCase {

  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10 );

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
    
    return c;
    
  }
  
  public void testMasterConfiguration() {
    
    POLRMasterDriver master = new POLRMasterDriver();

    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.debug_setConf(this.generateDebugConfigurationObject());
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

    // ------------------

    // test the base conf stuff ------------
    
    assertEquals( master.getConf().getInt("com.cloudera.knittingboar.setup.FeatureVectorSize", 0), 10 );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.LocalInputSplitPath"), "hdfs://127.0.0.1/input/0" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.PredictorLabelNames"), "x,y" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.PredictorVariableTypes"), "numeric,numeric" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.TargetVariableName"), "color" );
    assertEquals( master.getConf().get("com.cloudera.knittingboar.setup.ColumnHeaderNames"), "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias" );
  
    // now test the parsed stuff ------------
  
    
  
  
  }
  
  public void testIncomingGradientAccumulation() {
    
    
    
    
    POLRMasterDriver master = new POLRMasterDriver();

    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.debug_setConf(this.generateDebugConfigurationObject());
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
    // ------------------
    

    // now setup synthetic gradient messages --------
    
    double val1 = -1.0;
    double val2 = 1.0;
    
//    int numFeatures = 5;
     
     GradientBuffer g0 = new GradientBuffer( 2, master.FeatureVectorSize );
     g0.setCell(0, 0, val1);
     g0.setCell(0, 1, val2);
 

    GradientUpdateMessage msg0 = new GradientUpdateMessage("127.0.0.1", g0 );
    
    GradientBuffer g1 = new GradientBuffer( 2, master.FeatureVectorSize );
    g1.Copy(g0);
    
    GradientUpdateMessage msg1 = new GradientUpdateMessage("127.0.0.2", g1 );

    
    GradientBuffer g2 = new GradientBuffer( 2, master.FeatureVectorSize );
    g2.Copy(g0);
    
    GradientUpdateMessage msg2 = new GradientUpdateMessage("127.0.0.3", g2 );
    
    
    
    master.AddIncomingGradientMessageToQueue(msg0);
    
    
    master.AddIncomingGradientMessageToQueue(msg1);
    
    
    master.AddIncomingGradientMessageToQueue(msg2);
    
    master.RecvGradientMessage(); // process msg
    master.RecvGradientMessage(); // process msg
    master.RecvGradientMessage(); // process msg
    
    GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();
    
    System.out.println( "Master Param Vector: " + returned_msg.parameter_vector.get(0, 0) + ", " + returned_msg.parameter_vector.get(0, 1) );
    
    assertEquals( -1.0, returned_msg.parameter_vector.get(0, 0) );
    assertEquals( 1.0, returned_msg.parameter_vector.get(0, 1) );

    
    GlobalParameterVectorUpdateMessage returned_msg_1 = master.GetNextGlobalUpdateMsgFromQueue();
    
    System.out.println( "Master Param Vector: " + returned_msg_1.parameter_vector.get(0, 0) + ", " + returned_msg_1.parameter_vector.get(0, 1) );
    
    assertEquals( -2.0, returned_msg_1.parameter_vector.get(0, 0) );
    assertEquals( 2.0, returned_msg_1.parameter_vector.get(0, 1) );
    
    
    GlobalParameterVectorUpdateMessage returned_msg_2 = master.GetNextGlobalUpdateMsgFromQueue();
    
    System.out.println( "Master Param Vector: " + returned_msg_2.parameter_vector.get(0, 0) + ", " + returned_msg_2.parameter_vector.get(0, 1) );
    
    assertEquals( -3.0, returned_msg_2.parameter_vector.get(0, 0) );
    assertEquals( 3.0, returned_msg_2.parameter_vector.get(0, 1) );
    
    
  }
  
  public void testIncomingGradientTriggerOutgoingParamVecMsg() {
   
    
    POLRMasterDriver master = new POLRMasterDriver();
    //master.LoadConfig();
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    master.debug_setConf(this.generateDebugConfigurationObject());
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
    // ------------------

    
    double val1 = -1.0;
    double val2 = 1.0;
    
     
     GradientBuffer g0 = new GradientBuffer( 2, master.FeatureVectorSize );
     g0.setCell(0, 0, val1);
     g0.setCell(0, 1, val2);

     GradientUpdateMessage msg0 = new GradientUpdateMessage("127.0.0.1", g0 );
      
    
    master.AddIncomingGradientMessageToQueue(msg0);
    master.RecvGradientMessage(); // process msg
    
    
    
    GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();
    
    System.out.println( "Master Param Vector: " + returned_msg.parameter_vector.get(0, 0) + ", " + returned_msg.parameter_vector.get(0, 1) );
    
    assertEquals( -1.0, returned_msg.parameter_vector.get(0, 0) );
    assertEquals( 1.0, returned_msg.parameter_vector.get(0, 1) );    
    
  }
  
}
