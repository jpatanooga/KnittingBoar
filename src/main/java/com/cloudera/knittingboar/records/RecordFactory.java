package com.cloudera.knittingboar.records;

import java.util.List;
import java.util.Map;
import java.util.Set;

//import org.apache.mahout.classifier.sgd.RecordFactory;
import org.apache.mahout.math.Vector;

/**
 * Base interface for Knitting Boar's vectorization system
 * 
 * @author jpatterson
 *
 */
public interface RecordFactory {

  public static String TWENTYNEWSGROUPS_RECORDFACTORY = "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory";
  public static String RCV1_RECORDFACTORY = "com.cloudera.knittingboar.records.RCV1RecordFactory";
  public static String CSV_RECORDFACTORY = "com.cloudera.knittingboar.records.CSVRecordFactory";
  
  
  public int processLine(String line, Vector featureVector) throws Exception;
  
  public String GetClassnameByID(int id);

//  Map<String, Set<Integer>> getTraceDictionary();

//  RecordFactory includeBiasTerm(boolean useBias);

  public List<String> getTargetCategories();

  //void firstLine(String line);  
  
}
