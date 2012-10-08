package com.cloudera.knittingboar.yarn.appmaster;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.Updateable;

public abstract class AbstractComputableMaster<T extends Updateable> implements ComputableMaster<T> {
  
  private Configuration conf;
  
  public void setup(Configuration c) {
    conf = c;
  }
}