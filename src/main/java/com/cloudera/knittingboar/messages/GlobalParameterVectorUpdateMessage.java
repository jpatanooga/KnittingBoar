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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.jboss.netty.buffer.ChannelBuffer;

public class GlobalParameterVectorUpdateMessage {
  
  public String dst_host = "";
  
  public Matrix parameter_vector = null;
  
  public int GlobalPassCount = 0; // what pass should the worker dealing with?
  
  /**
   * message that is sent from the POLRMasterDriver to the POLRWorkerDrive
   * 
   * numCategories has to be 2 in the case of logistic regression
   * 
   * @param response_dst_host
   * @param numCategories
   * @param numFeatures
   */
  public GlobalParameterVectorUpdateMessage(String response_dst_host,
      int numCategories, int numFeatures) {
    
    this.dst_host = response_dst_host;
    
    // beta = new DenseMatrix(numCategories - 1, numFeatures);
    this.parameter_vector = new DenseMatrix(numCategories - 1, numFeatures);
    
  }
  
  public void Serialize(DataOutput d) throws IOException {
    
    d.writeInt(GlobalPassCount);
    // buf.write
    MatrixWritable.writeMatrix(d, this.parameter_vector);
    // MatrixWritable.
    
  }
  
}
