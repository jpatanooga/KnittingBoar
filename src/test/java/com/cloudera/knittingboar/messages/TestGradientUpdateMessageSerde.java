package com.cloudera.wovenwabbit.messages;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.cloudera.wovenwabbit.sgd.GradientBuffer;

import junit.framework.TestCase;

public class TestGradientUpdateMessageSerde extends TestCase {
  
  public static String msg_file = "/tmp/TestGradientUpdateMessageSerde.msg";
  
  public static String ip = "255.255.255.1";
  
  public static int pass_count = 8;
  
  public void testSerde() throws IOException {
    
    int classes = 20;
    int features = 10000;
    
    GradientBuffer g = new GradientBuffer( classes, features );
    
    g.numFeatures();
  
    for (int c = 0; c < classes - 1; c++) {
      
      for (int f = 0; f < features; f++ ) {
    
        g.setCell(c, f, (double)((double)f / 10.0f) );
        
      }
      
    }

    System.out.println( "buffer created..." );
    
    GradientUpdateMessage msg = new GradientUpdateMessage( ip, g );
    msg.SrcWorkerPassCount = pass_count;
    
    
    
    assertEquals( 10000, msg.gradient.numFeatures() );
    
    assertEquals( 20, msg.gradient.numCategories() );
    
    
    //ByteArrayOutputStream bas = new ByteArrayOutputStream();
    OutputStream modelOutput = new FileOutputStream(msg_file);
    DataOutput d = new DataOutputStream(modelOutput);

    msg.Serialize(d);
    
    DataInput in = new DataInputStream(new FileInputStream(msg_file));
    
    GradientUpdateMessage msg_deser = new GradientUpdateMessage( "", new GradientBuffer( classes, features ) );
    msg_deser.Deserialize(in);
    
    assertEquals( ip, msg_deser.src_host );
    assertEquals( pass_count, msg_deser.SrcWorkerPassCount );
    assertEquals( 0.1, msg_deser.gradient.getCell(0, 1) );
    assertEquals( 0.2, msg_deser.gradient.getCell(0, 2) );
    assertEquals( 0.3, msg_deser.gradient.getCell(0, 3) );
    assertEquals( 0.4, msg_deser.gradient.getCell(0, 4) );
    assertEquals( 0.5, msg_deser.gradient.getCell(0, 5) );
    
  }

}
