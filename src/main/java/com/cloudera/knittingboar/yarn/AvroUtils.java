package com.cloudera.wovenwabbit.yarn;

import com.cloudera.wovenwabbit.yarn.avro.generated.WorkerId;

public class AvroUtils {
  public static String getWorkerId(WorkerId workerId) {
    return new String(workerId.bytes());
  }
  
  public static WorkerId createWorkerId(String name) {
    byte[] buff = new byte[32];
    System.arraycopy(name.getBytes(), 0, buff, 0, name.length());
    
    return new WorkerId(buff);
  }
}
