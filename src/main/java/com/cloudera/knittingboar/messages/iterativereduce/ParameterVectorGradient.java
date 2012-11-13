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

package com.cloudera.knittingboar.messages.iterativereduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

import com.cloudera.knittingboar.sgd.GradientBuffer;

public class ParameterVectorGradient {

  // worker stuff to send out
  public int SrcWorkerPassCount = 0;   
  //public GradientBuffer worker_gradient = null;

  // sending out, this is just the delta on the worker gradient (master will accumulate)
  // coming back, this is hte new parameter vector to replace ours with
  public Matrix parameter_vector = null;
  public int GlobalPassCount = 0; // what pass should the worker dealing with?

  public int TrainedRecords = 0;
  public float AvgLogLikelihood = 0;
  public float PercentCorrect = 0;

  
  public byte[] Serialize() throws IOException {
    
//    DataOutput d
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput d = new DataOutputStream( out );

    
    //d.writeUTF(src_host);
    d.writeInt(this.SrcWorkerPassCount);
    d.writeInt(this.GlobalPassCount);
    
    d.writeInt(this.TrainedRecords);
    d.writeFloat(this.AvgLogLikelihood);
    d.writeFloat(this.PercentCorrect);
    //buf.write
    //MatrixWritable.writeMatrix(d, this.worker_gradient.getMatrix());
    MatrixWritable.writeMatrix(d, this.parameter_vector);
    //MatrixWritable.
    
    return out.toByteArray();
  }
  
  public void Deserialize(byte[] bytes ) throws IOException {
      //DataInput in) throws IOException {
    
    ByteArrayInputStream b = new ByteArrayInputStream(bytes);
    DataInput in = new DataInputStream(b);
    //this.src_host = in.readUTF();
    this.SrcWorkerPassCount = in.readInt();
    this.GlobalPassCount = in.readInt();
    
    this.TrainedRecords = in.readInt(); //d.writeInt(this.TrainedRecords);
    this.AvgLogLikelihood = in.readFloat(); //d.writeFloat(this.AvgLogLikelihood);
    this.PercentCorrect = in.readFloat(); //d.writeFloat(this.PercentCorrect);
    
    
    this.parameter_vector = MatrixWritable.readMatrix(in);
    
  }
  
  public int numFeatures() {
    return this.parameter_vector.numCols();
  }

  public int numCategories() {
    return this.parameter_vector.numRows();
  }
  
  
}
