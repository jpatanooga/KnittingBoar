package com.cloudera.wovenwabbit.messages;

import com.cloudera.wovenwabbit.sgd.GradientBuffer;

import junit.framework.TestCase;

public class TestGradientUpdateMessage extends TestCase {

  
  
  public void testCtor() {
    
    GradientBuffer buf = new GradientBuffer( 2, 5 );
    
    System.out.println( "buffer created..." );
    
    GradientUpdateMessage msg = new GradientUpdateMessage("127.0.0.1", buf );
    
    assertEquals( 5, msg.gradient.numFeatures() );
    
    assertEquals( 2, msg.gradient.numCategories() );
    
    System.out.println( "Test Complete for Ctor()" );
    
  }
  
  
}
