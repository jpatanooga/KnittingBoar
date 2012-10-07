package com.cloudera.wovenwabbit.yarn.appmaster;

import java.util.Collection;

import com.cloudera.wovenwabbit.yarn.Updateable;


public interface ComputableMaster<T extends Updateable> {
  T compute(Collection<T> workerUpdates, Collection<T> masterUpdates);
  T getResults();
}
