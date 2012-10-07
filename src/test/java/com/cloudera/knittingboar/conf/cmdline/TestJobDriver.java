package com.cloudera.wovenwabbit.conf.cmdline;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.mahout.classifier.sgd.TrainLogistic;

import junit.framework.TestCase;

public class TestJobDriver extends TestCase {

  public void testBasics() throws Exception {
    
    
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    String[] params = new String[]{
        "--input", "donut.csv",
        "--output", "foo.model",
        "--features", "20",
        "--passes", "100",
        "--rate", "50"
    };
    
    ModelTrainerCmdLineDriver.mainToOutput(params, pw);
    
    String trainOut = sw.toString();
    assertTrue(trainOut.contains("Parse:correct"));
    
    
  }
  
  
  
}
