package com.cloudera.wovenwabbit.utils;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

public class TestDatasetConverter extends TestCase {

  public void test20NewsgroupsFormatConverterForNWorkers() throws IOException {
    
    //DatasetConverter.ConvertNewsgroupsFromNaiveBayesFormat("/Users/jpatterson/Downloads/datasets/20news-processed/train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train2/");
    
    //int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train3/", 5657);
    
    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/", 2850);
    
    assertEquals( 11314, count );
    
  }
  
}
