package com.cloudera.knittingboar.utils;

import java.io.IOException;

import junit.framework.TestCase;

public class TestRcv1SubsetConversion extends TestCase {

  String file = "/Users/jpatterson/Downloads/rcv1/rcv1.train.vw";
  
  public void testConversion() throws IOException {
    
    
    int c = DatasetConverter.ExtractSubsetofRCV1V2ForTraining(file, "/Users/jpatterson/Downloads/rcv1/subset/train-unit-test/", 100000, 10000);

    System.out.println( "count: " + c);
    
    //assertEquals( 11314, count );
    
    
  }
  
}
