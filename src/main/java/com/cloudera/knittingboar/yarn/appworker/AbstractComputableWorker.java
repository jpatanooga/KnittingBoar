package com.cloudera.knittingboar.yarn.appworker;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.Updateable;

public abstract class AbstractComputableWorker<T extends Updateable, R> implements
    ComputableWorker<T, R> {
  
  private Configuration conf;
  
  public void setup(Configuration c) {
    conf = c;
  }
}