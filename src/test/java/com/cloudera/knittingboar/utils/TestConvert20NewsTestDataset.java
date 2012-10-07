package com.cloudera.wovenwabbit.utils;

import java.io.IOException;

import junit.framework.TestCase;

public class TestConvert20NewsTestDataset extends TestCase {


  public void testNaiveBayesFormatConverter() throws IOException {
        
    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train-dataset-unit-test/", 21000);
    
    //int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/", 2850);
    
    System.out.println( "Total: " + count );
    
    assertEquals( 11314, count );
    
  }  
  
  
}
