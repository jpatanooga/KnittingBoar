package com.cloudera.knittingboar.utils;

import java.util.List;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

public class TestUtils extends TestCase {
  
  public void testStringStuff() {
    
    String s = "x,y,shape,color,k,k0,xx,xy,yy,a,b,c,bias";
    
    String[] ar = s.split(",");
    
    List<String> values = Lists.newArrayList(s.split(","));
    
    System.out.println( ">" + ar.length);
    
    
    System.out.println( ">" + values.size());
    
  }

}
