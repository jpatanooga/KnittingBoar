package com.cloudera.knittingboar.yarn.appmaster;

import java.io.DataOutputStream;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.Updateable;

public interface ComputableMaster<T extends Updateable> {
  void setup(Configuration c);
  void complete(DataOutputStream out);
  T compute(Collection<T> workerUpdates, Collection<T> masterUpdates);
  T getResults();
}