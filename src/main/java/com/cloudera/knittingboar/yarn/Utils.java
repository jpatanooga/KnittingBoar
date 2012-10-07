package com.cloudera.knittingboar.yarn;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;

/*
 * TODO: fix to more sane method of copying collections (e.g. Collections.addAll
 * 
 */
public class Utils {

  public static String getWorkerId(WorkerId workerId) {
    return new String(workerId.bytes());
  }

  public static WorkerId createWorkerId(String name) {
    byte[] buff = new byte[32];
    System.arraycopy(name.getBytes(), 0, buff, 0, name.length());

    return new WorkerId(buff);
  }

  public static void mergeConfigs(StartupConfiguration sourceConf,
      Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;

    Map<CharSequence, CharSequence> confMap = sourceConf.getOther();

    if (confMap == null)
      return;

    for (Map.Entry<CharSequence, CharSequence> entry : confMap.entrySet()) {
      destConf.set(entry.getKey().toString(), entry.getValue().toString());
    }
  }

  public static void mergeConfigs(Configuration sourceConf, Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;

    for (Map.Entry<String, String> entry : sourceConf) {
      if (destConf.get(entry.getKey()) == null) {
        destConf.set(entry.getKey(), entry.getValue());
      }
    }
  }
  
  public static void mergeConfigs(Map<String, String> sourceConf, Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;
    
    for(Map.Entry<String, String> entry : sourceConf.entrySet()) {
      if (destConf.get(entry.getKey()) == null) {
        destConf.set(entry.getKey(), entry.getValue());
      }
    }
  }
  
  public static void mergeConfigs(Map<String, String> sourceConf, StartupConfiguration destConf) {
    if (sourceConf == null || destConf == null)
      return;
    
    Map<CharSequence, CharSequence> confMap = destConf.getOther();
    
    if (confMap == null)
      confMap = new HashMap<CharSequence, CharSequence>();
    
    for (Map.Entry<String, String> entry : sourceConf.entrySet()) {
      if (! confMap.containsKey(entry.getKey()))
        confMap.put(entry.getKey(), entry.getValue());
    }
    
    destConf.setOther(confMap);
  }
}