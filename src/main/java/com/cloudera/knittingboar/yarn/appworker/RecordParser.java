package com.cloudera.wovenwabbit.yarn.appworker;

public interface RecordParser<T> {
  void reset();
  void parse();
  void setFile(String file, long offset, long length);
  void setFile(String file);
  boolean hasMoreRecords();
  T nextRecord();
}