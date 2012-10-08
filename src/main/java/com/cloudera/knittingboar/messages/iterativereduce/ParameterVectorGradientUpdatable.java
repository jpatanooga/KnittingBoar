package com.cloudera.knittingboar.messages.iterativereduce;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.mahout.math.Matrix;

import com.cloudera.knittingboar.sgd.GradientBuffer;
import com.cloudera.knittingboar.yarn.Updateable;

public class ParameterVectorGradientUpdatable implements Updateable<ParameterVectorGradient> {

  ParameterVectorGradient param_msg = null;
  
  
  
  @Override
  public void fromBytes(ByteBuffer b) {
    // TODO Auto-generated method stub
    try {
      this.param_msg.Deserialize(b.array());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  @Override
  public ParameterVectorGradient get() {
    // TODO Auto-generated method stub
    return this.param_msg;
  }
  @Override
  public void set(ParameterVectorGradient t) {
    // TODO Auto-generated method stub
    this.param_msg = t;
  }
  @Override
  public ByteBuffer toBytes() {
    // TODO Auto-generated method stub
    byte[] bytes = null;
    try {
      bytes = this.param_msg.Serialize();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    ByteBuffer buf = ByteBuffer.allocate(bytes.length);
    buf.put(bytes);
    
    return buf;
  }
  
  
}
