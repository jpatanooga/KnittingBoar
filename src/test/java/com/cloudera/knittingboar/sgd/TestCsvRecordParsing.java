package com.cloudera.wovenwabbit.sgd;

import java.util.ArrayList;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

/**
 * This explodes if you also have the google-collections jar in the classpath.
 * 
 * fix: remove google-collections
 * 
 * @author jpatterson
 *
 */
public class TestCsvRecordParsing extends TestCase {

  
  public void testParse() {
    
    String line = "\"a\", \"b\", \"c\"\n";
    
    Splitter COMMA = Splitter.on(',').trimResults(CharMatcher.is('"'));
    ArrayList<String> variableNames = Lists.newArrayList(COMMA.split(line));

    
    System.out.println( variableNames );
    
  }
  
}
