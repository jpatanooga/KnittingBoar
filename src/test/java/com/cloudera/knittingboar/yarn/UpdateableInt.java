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
    b.rewind();
    
    return b;
    
  }

  @Override
  public void fromBytes(ByteBuffer b) {
    b.rewind();
    i = b.getInt();
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
