package com.cloudera.knittingboar.yarn;

import java.nio.ByteBuffer;

public interface Updateable<T> {
  ByteBuffer toBytes();
  void fromBytes(ByteBuffer b);
  void fromString(String s);
  T get();
  void set(T t);
}