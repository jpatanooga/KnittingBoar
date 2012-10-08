package com.cloudera.knittingboar.messages;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import junit.framework.TestCase;

import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradient;
import com.cloudera.knittingboar.sgd.GradientBuffer;


public class TestParameterVectorGradient extends TestCase {

  
  
  public static String msg_file = "/tmp/TestGradientUpdateMessageSerde.msg";
  
  //public static String ip = "255.255.255.1";
  
  public static int pass_count = 8;
  
  public void testSerde() throws IOException {
    
    int classes = 20;
    int features = 10000;
    
    //GradientBuffer g = new GradientBuffer( classes, features );
    Matrix m = new DenseMatrix(classes, features);
    //m.set(0, 0, 0.1);
    //m.set(0, 1, 0.3);
    
    
    //g.numFeatures();
  
    for (int c = 0; c < classes - 1; c++) {
      
      for (int f = 0; f < features; f++ ) {
    
        m.set(c, f, (double)((double)f / 10.0f) );
        
      }
      
    }

    System.out.println( "matrix created..." );
    
    //GradientUpdateMessage msg = new GradientUpdateMessage( ip, g );
    //msg.SrcWorkerPassCount = pass_count;
    
    ParameterVectorGradient vec_gradient = new ParameterVectorGradient();
    vec_gradient.SrcWorkerPassCount = pass_count;
    vec_gradient.parameter_vector = m;
    
    
    assertEquals( 10000, vec_gradient.numFeatures() );
    assertEquals( 10000, vec_gradient.parameter_vector.columnSize() );
    
    assertEquals( 20, vec_gradient.numCategories() );
    assertEquals( 20, vec_gradient.parameter_vector.rowSize() );
    
    //ByteArrayOutputStream bas = new ByteArrayOutputStream();
//    OutputStream modelOutput = new FileOutputStream(msg_file);
//    DataOutput d = new DataOutputStream(modelOutput);

    //msg.Serialize(d);
    byte[] buf = vec_gradient.Serialize();
    
    //DataInput in = new DataInputStream(new FileInputStream(msg_file));
    
    //GradientUpdateMessage msg_deser = new GradientUpdateMessage( "", new GradientBuffer( classes, features ) );
    //msg_deser.Deserialize(in);
    
    ParameterVectorGradient vec_gradient_deserialized = new ParameterVectorGradient();
    vec_gradient_deserialized.Deserialize(buf);
    
    //assertEquals( ip, msg_deser.src_host );
    assertEquals( pass_count, vec_gradient_deserialized.SrcWorkerPassCount );
    assertEquals( 0.1, vec_gradient_deserialized.parameter_vector.get(0, 1) );
    assertEquals( 0.2, vec_gradient_deserialized.parameter_vector.get(0, 2) );
    assertEquals( 0.3, vec_gradient_deserialized.parameter_vector.get(0, 3) );
    assertEquals( 0.4, vec_gradient_deserialized.parameter_vector.get(0, 4) );
    assertEquals( 0.5, vec_gradient_deserialized.parameter_vector.get(0, 5) );
    
  }
  
  
  
  
}
