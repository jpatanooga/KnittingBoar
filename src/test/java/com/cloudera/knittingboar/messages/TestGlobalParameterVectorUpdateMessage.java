package com.cloudera.knittingboar.messages;

import junit.framework.TestCase;

public class TestGlobalParameterVectorUpdateMessage extends TestCase {

  
  public void testCtor() {
    
    GlobalParameterVectorUpdateMessage msg = new GlobalParameterVectorUpdateMessage("127.0.0.1", 2, 5);
    
    assertEquals( 5, msg.parameter_vector.numCols() );
    
    assertEquals( 1, msg.parameter_vector.rowSize() );
    
    System.out.println( "Test Complete for Ctor()" );
    
  }
  
}
