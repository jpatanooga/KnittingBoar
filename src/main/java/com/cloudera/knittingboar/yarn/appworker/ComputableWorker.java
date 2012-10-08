package com.cloudera.knittingboar.yarn.appworker;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.Updateable;

public interface ComputableWorker<T extends Updateable, R> {
  void setup(Configuration c);
  T compute(List<R> records);
  T getResults();
  void update(T t);
}