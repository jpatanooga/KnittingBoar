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

package com.cloudera.knittingboar.messages;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import junit.framework.TestCase;

import com.cloudera.knittingboar.messages.iterativereduce.ParameterVector;



public class TestParameterVector extends TestCase {

  
  
  public static String msg_file = "/tmp/TestGradientUpdateMessageSerde.msg";
  
  //public static String ip = "255.255.255.1";
  
  public static int pass_count = 8;
  
  public void testSerde() throws IOException {
    
    int classes = 20;
    int features = 10000;
    
    //GradientBuffer g = new GradientBuffer( classes, features );
    Matrix m = new DenseMatrix(classes, features);
    //m.set(0, 0, 0.1);
    //m.set(0, 1, 0.3);
    
    
    //g.numFeatures();
  
    for (int c = 0; c < classes - 1; c++) {
      
      for (int f = 0; f < features; f++ ) {
    
        m.set(c, f, (double)((double)f / 10.0f) );
        
      }
      
    }

    System.out.println( "matrix created..." );
    
    
    ParameterVector vec_gradient = new ParameterVector();
    vec_gradient.SrcWorkerPassCount = pass_count;
    vec_gradient.parameter_vector = m;
    vec_gradient.AvgLogLikelihood = -1.368f;
    vec_gradient.PercentCorrect = 72.68f;
    vec_gradient.TrainedRecords = 2500;
    
    
    assertEquals( 10000, vec_gradient.numFeatures() );
    assertEquals( 10000, vec_gradient.parameter_vector.columnSize() );
    
    assertEquals( 20, vec_gradient.numCategories() );
    assertEquals( 20, vec_gradient.parameter_vector.rowSize() );
    
    byte[] buf = vec_gradient.Serialize();
    
    
    ParameterVector vec_gradient_deserialized = new ParameterVector();
    vec_gradient_deserialized.Deserialize(buf);
    
    assertEquals( pass_count, vec_gradient_deserialized.SrcWorkerPassCount );
    assertEquals( 0.1, vec_gradient_deserialized.parameter_vector.get(0, 1) );
    assertEquals( 0.2, vec_gradient_deserialized.parameter_vector.get(0, 2) );
    assertEquals( 0.3, vec_gradient_deserialized.parameter_vector.get(0, 3) );
    assertEquals( 0.4, vec_gradient_deserialized.parameter_vector.get(0, 4) );
    assertEquals( 0.5, vec_gradient_deserialized.parameter_vector.get(0, 5) );
    
    assertEquals( -1.368f, vec_gradient_deserialized.AvgLogLikelihood );
    assertEquals( 72.68f, vec_gradient_deserialized.PercentCorrect );
    assertEquals( 2500, vec_gradient_deserialized.TrainedRecords );
    
  }
  
  
  
  
}
