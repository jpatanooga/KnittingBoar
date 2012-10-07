package com.cloudera.knittingboar.yarn.appworker;

import java.util.List;

import com.cloudera.knittingboar.yarn.Updateable;

public interface ComputableWorker<T extends Updateable, R> {
  public T compute(List<R> records);
  public T getResults();
  void update(T t);
}