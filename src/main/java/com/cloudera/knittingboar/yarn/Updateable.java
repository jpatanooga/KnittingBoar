package com.cloudera.wovenwabbit.yarn;

import java.nio.ByteBuffer;

public interface Updateable<T> {
  ByteBuffer toBytes();
  void fromBytes(ByteBuffer b);
  T get();
  void set(T t);
}