package com.cloudera.knittingboar.yarn;

import java.nio.ByteBuffer;

public class UpdateableInt implements Updateable<Integer> {
  private int i = 0;
  private ByteBuffer b;
  
  @Override
  public ByteBuffer toBytes() {
    if (b == null)
      b = ByteBuffer.allocate(4);
    
    b.clear();
    b.putInt(i);
    
    return b;
    
  }

  @Override
  public void fromBytes(ByteBuffer b) {
    i = b.getInt();
  }
  
  @Override
  public void fromString(String s) {
    i = Integer.parseInt(s);
  }

  @Override
  public Integer get() {
    return i;
  }

  @Override
  public void set(Integer t) {
    i = t;
  }
}
