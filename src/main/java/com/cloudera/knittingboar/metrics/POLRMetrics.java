package com.cloudera.knittingboar.metrics;

/**
 * this is the class we'll use to report worker node perf to master node
 * 
 * @author jpatterson
 *
 */
public class POLRMetrics {
  
  public String WorkerNodeIPAddress = null;
  public String WorkerNodeInputDataSplit = null;
  
  public long AvgBatchProecssingTimeInMS = 0;
  
  public long TotalInputProcessingTimeInMS = 0;
  public long TotalRecordsProcessed = 0;
  
  public double AvgLogLikelihood = 0.0;
  public double AvgCorrect = 0.0;

}
