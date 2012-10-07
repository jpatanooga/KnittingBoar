package com.cloudera.knittingboar.yarn.appmaster;

import java.util.Collection;

import com.cloudera.wovenwabbit.yarn.Updateable;


public interface ComputableMaster<T extends Updateable> {
  T compute(Collection<T> workerUpdates, Collection<T> masterUpdates);
  T getResults();
}
