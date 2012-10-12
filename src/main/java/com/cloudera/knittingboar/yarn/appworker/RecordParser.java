package com.cloudera.knittingboar.yarn.appworker;

import com.cloudera.knittingboar.yarn.Updateable;

public interface RecordParser<T extends Updateable> {
  void reset();
  void parse();
  void setFile(String file, long offset, long length);
  void setFile(String file);
  boolean hasMoreRecords();
  T nextRecord();
  int getCurrentRecordsProcessed();
}