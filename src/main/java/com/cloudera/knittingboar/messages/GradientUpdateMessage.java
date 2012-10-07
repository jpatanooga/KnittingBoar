package com.cloudera.knittingboar.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.MatrixWritable;

import com.cloudera.knittingboar.sgd.GradientBuffer;

public class GradientUpdateMessage {

  public String src_host = "";
  public int SrcWorkerPassCount = 0; // what pass is the worker dealing with?
  
  public GradientBuffer gradient = null;
  
  /**
   * Gradient message to send to master process
   * - takes a snapshot of the gradient to send
   * 
   * 
   * @param remote_src_host
   * @param buf
   */
  public GradientUpdateMessage( String remote_src_host, GradientBuffer buf ) {
    
    this.src_host = remote_src_host;
    
    if (null == buf ) {
      System.out.println( "ERR > null ctor buffer!" );
    }
    
    //System.out.println( "numCat: " + buf.numCategories() ); 
    
    this.gradient = new GradientBuffer( buf.numCategories(), buf.numFeatures() );
    // get a snapshot of the current state of the gradient
    this.gradient.Copy(buf);
    
    
  }
  
  public void Serialize(DataOutput d) throws IOException {
    
    d.writeUTF(src_host);
    d.writeInt(this.SrcWorkerPassCount);
    //buf.write
    MatrixWritable.writeMatrix(d, this.gradient.getMatrix());
    //MatrixWritable.
    
  }
  
  public void Deserialize(DataInput in) throws IOException {
    
    this.src_host = in.readUTF();
    this.SrcWorkerPassCount = in.readInt();
    this.gradient.setMatrix( MatrixWritable.readMatrix(in) );
    
  }
  
  
}
