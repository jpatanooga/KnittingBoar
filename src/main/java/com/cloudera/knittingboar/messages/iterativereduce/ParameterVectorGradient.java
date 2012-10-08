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
/*
  private int numFeatures = -1;
  private int numCategories = -1;
  
  public ParameterVectorGradient(int numFeatures, int numCategories ) {
    this.numCategories = numCategories;
    this.numFeatures = numFeatures;
  }
  */
  public byte[] Serialize() throws IOException {
    
//    DataOutput d
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput d = new DataOutputStream( out );

    
    //d.writeUTF(src_host);
    d.writeInt(this.SrcWorkerPassCount);
    d.writeInt(this.GlobalPassCount);
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
    this.parameter_vector = MatrixWritable.readMatrix(in);
    
  }
  
  public int numFeatures() {
    return this.parameter_vector.numCols();
  }

  public int numCategories() {
    return this.parameter_vector.numRows();
  }
  
  
}
