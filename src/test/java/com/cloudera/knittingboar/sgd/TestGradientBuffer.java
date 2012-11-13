/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.knittingboar.sgd;

import junit.framework.TestCase;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import com.cloudera.knittingboar.utils.Utils;

public class TestGradientBuffer extends TestCase {


  public void testBasic() {
  
    GradientBuffer g = new GradientBuffer( 2, 5 );
    
    g.numFeatures();
    
    g.setCell(0, 0, 0.4);
    g.setCell(0, 1, 0.3);
    g.setCell(0, 2, 0.2);
    g.setCell(0, 3, 0.1);
    g.setCell(0, 4, 0.0);
    
    assertEquals( g.numFeatures(), 5 );
    
    assertEquals( g.getCell(0, 0), 0.4 );
    assertEquals( g.getCell(0, 2), 0.2 );
    assertEquals( g.getCell(0, 4), 0.0 );
    
    
    System.out.println("done!");
    
    
    assertNotNull(0);
    
    
  }

  public void testBasic_Category4() {
    
    GradientBuffer g = new GradientBuffer( 4, 5 );
    
    g.numFeatures();
    
    g.setCell(1, 0, 0.4);
    g.setCell(1, 1, 0.3);
    g.setCell(1, 2, 0.2);
    g.setCell(1, 3, 0.1);
    g.setCell(1, 4, 0.0);
    
    assertEquals( g.numFeatures(), 5 );
    
    assertEquals( g.getCell(1, 0), 0.4 );
    assertEquals( g.getCell(1, 2), 0.2 );
    assertEquals( g.getCell(1, 4), 0.0 );
    
    
    System.out.println("done!");
    
    
    assertNotNull(0);
    
    
  }

  public void testAccumulateGradient() {
  
    GradientBuffer g0 = new GradientBuffer( 2, 2 );
    
    g0.setCell(0, 0, 0.4);
    g0.setCell(0, 1, 0.3);
    
    assertEquals( g0.numFeatures(), 2 );

    
    
    GradientBuffer g1 = new GradientBuffer( 2, 2 );
    
    g1.setCell(0, 0, 0.1);
    g1.setCell(0, 1, 0.3);
    
    assertEquals( g1.numFeatures(), 2 );
    
    
    
    g0.Accumulate(g1);
    
    // check source
    assertEquals( g1.getCell(0, 0), 0.1 );
    // check accumlation in g0
    assertEquals( g0.getCell(0, 0), 0.5 );

    // check source
    assertEquals( g1.getCell(0, 1), 0.3 );
    // check accumlation in g0
    assertEquals( g0.getCell(0, 1), 0.6 );
    
    
    System.out.println("matrix accumulation test done!");
    
    
    assertNotNull(0);
    
    
  }
  
  public void testAccumulateGradientMatrix() {
    
    GradientBuffer g0 = new GradientBuffer( 2, 2 );
    
    g0.setCell(0, 0, 0.4);
    g0.setCell(0, 1, 0.3);
    
    assertEquals( g0.numFeatures(), 2 );

    
    
/*    GradientBuffer g1 = new GradientBuffer( 2, 2 );
    
    g1.setCell(0, 0, 0.1);
    g1.setCell(0, 1, 0.3);
    
    assertEquals( g1.numFeatures(), 2 );
*/
    Matrix m = new DenseMatrix(2, 2);
    m.set(0, 0, 0.1);
    m.set(0, 1, 0.3);
    
    
    g0.AccumulateGradient(m);
    //m.get(arg0, arg1)
    // check source
    assertEquals( m.get(0, 0), 0.1 );
    // check accumlation in g0
    assertEquals( g0.getCell(0, 0), 0.5 );

    // check source
    assertEquals( m.get(0, 1), 0.3 );
    // check accumlation in g0
    assertEquals( g0.getCell(0, 1), 0.6 );
    
    
    System.out.println("matrix accumulation test done!");
    
    
    assertNotNull(0);
    
    
  }  
  
  
  
  
  public void testSetGradientCell() {
    
    double val = -1.5811388300841898;
    
    GradientBuffer g0 = new GradientBuffer( 2, 2 );
    
    g0.setCell(0, 0, val);
 //   g0.setCell(0, 1, 0.3);
    
    assertEquals( g0.getCell(0, 0), val );

    
    
  }
  
  
  public void testMergeGradient() {
    
    
    
  }  
  
  public void testCopy() {
    
   double val1 = -1.5811388300841898;
   double val2 = 9.5811388300841898;
    
    GradientBuffer g0 = new GradientBuffer( 2, 2 );
    g0.setCell(0, 0, val1);
    g0.setCell(0, 1, val2);
    
    GradientBuffer g1 = new GradientBuffer( 2, 2 );
    
    g1.Copy(g0);
    
    assertEquals( g1.getCell(0, 0), val1 );
    assertEquals( g1.getCell(0, 1), val2 );
    
    System.out.println( "copy test complete" );
    
  }
  
  public void testAverageGradientBuffer() {
    
    System.out.println( "testAverageGradientBuffer --------" ); 
    
    GradientBuffer g0 = new GradientBuffer( 2, 2 );
    
    g0.setCell(0, 0, 0.1d);
    g0.setCell(0, 1, 0.5d);
    
    assertEquals( g0.numFeatures(), 2 );

    Matrix m = new DenseMatrix(2, 2);
    m.set(0, 0, 0.5d);
    m.set(0, 1, 0.1d);
    
    
    g0.AccumulateGradient(m);
    //m.get(arg0, arg1)
    // check source
    assertEquals( m.get(0, 0), 0.5d );
    // check accumlation in g0
    //assertEquals( g0.getCell(0, 0), 0.6 );

    junit.framework.Assert.assertEquals( 0.6d, g0.getCell(0, 0), 0.0001);
    
    // check source
    assertEquals( m.get(0, 1), 0.1d );
    // check accumlation in g0
//    assertEquals( g0.getCell(0, 1), 0.6 );
    
    Utils.PrintVectorNonZero(g0.gamma.viewRow(0));
    //Utils.PrintVectorNonZero(g0.gamma.viewRow(1));
    
    g0.AverageAccumulations(2);
    
    Utils.PrintVectorNonZero(g0.gamma.viewRow(0));
    
    System.out.println("matrix accumulation AVG test done!");
    
    //System.out.println( "add test: " + ( 10.0d + 6.0d ) );
    
    
    assertNotNull(0);    
    
  }
}
