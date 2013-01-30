package com.cloudera.knittingboar.records;

import java.util.Iterator;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

public class SVMLightUtils {
  
  public static String ConvertVectorIntoSVMLightFormat( Vector v, String target_value ) {
    
    StringBuilder sb = new StringBuilder(target_value + " |f");
    
    SequentialAccessSparseVector seq_vec = new SequentialAccessSparseVector(v);
    
    //boolean first = true;
    Iterator<Vector.Element> nonZeros = seq_vec.iterateNonZero();
    while (nonZeros.hasNext()) {
      Vector.Element vec_loc = nonZeros.next();
      
/*      if (!first) {
        System.out.print(",");
      } else {
        first = false;
      }
      */
      
      //System.out.print(" " + vec_loc.get());
      sb.append(" " + vec_loc.index() + ":" + vec_loc.get());
      
    }
    
    //System.out.println("");
    
    return sb.toString();
    
  }

}
