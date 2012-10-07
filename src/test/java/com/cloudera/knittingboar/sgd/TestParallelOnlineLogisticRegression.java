package com.cloudera.knittingboar.sgd;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.utils.Utils;

/**
 * Mostly temporary tests used to debug components as we developed the system
 * 
 * @author jpatterson
 *
 */
public class TestParallelOnlineLogisticRegression extends TestCase {

  
  public void testCreateLR() {
    
    int categories = 2;
    int numFeatures = 5;
    double lambda = 1.0e-4;
    double learning_rate = 50;
    
    
    ParallelOnlineLogisticRegression plr = new ParallelOnlineLogisticRegression( categories, numFeatures, new L1())
    .lambda(lambda)
    .learningRate(learning_rate)
    .alpha(1 - 1.0e-3);
    
    assertEquals( plr.getLambda(), 1.0e-4 );
  }
  
  public void testTrainMechanics() {
    
    
    int categories = 2;
    int numFeatures = 5;
    double lambda = 1.0e-4;
    double learning_rate = 10;
    
    ParallelOnlineLogisticRegression plr = new ParallelOnlineLogisticRegression( categories, numFeatures, new L1())
    .lambda(lambda)
    .learningRate(learning_rate)
    .alpha(1 - 1.0e-3);
    
        
    Vector input = new RandomAccessSparseVector(numFeatures);
    
    for ( int x = 0; x < numFeatures; x++ ) {
    
      input.set(x, x);
      
    }
    
    plr.train(0, input);
    
    plr.train(0, input);
    
    plr.train(0, input);
    
    
  }

  
  public void testPOLRInternalBuffers() {
    
    System.out.println( "testPOLRInternalBuffers --------------" );
    
    int categories = 2;
    int numFeatures = 5;
    double lambda = 1.0e-4;
    double learning_rate = 10;

    ArrayList<Vector> trainingSet_0 = new ArrayList<Vector>();
    
    for ( int s = 0; s < 1; s++ ) {
      
      
      Vector input = new RandomAccessSparseVector(numFeatures);
      
      for ( int x = 0; x < numFeatures; x++ ) {
      
        input.set(x, x);
        
      }
      
      trainingSet_0.add(input);

    } // for
    
    ParallelOnlineLogisticRegression plr_agent_0 = new ParallelOnlineLogisticRegression( categories, numFeatures, new L1())
    .lambda(lambda)
    .learningRate(learning_rate)
    .alpha(1 - 1.0e-3);
    
    System.out.println( "Beta: " );
    //Utils.PrintVectorNonZero(plr_agent_0.getBeta().getRow(0));
    Utils.PrintVectorNonZero(plr_agent_0.getBeta().viewRow(0));
    
    
    System.out.println( "\nGamma: " );
    //Utils.PrintVectorNonZero(plr_agent_0.gamma.getMatrix().getRow(0));
    Utils.PrintVectorNonZero(plr_agent_0.gamma.getMatrix().viewRow(0));
  
    plr_agent_0.train(0, trainingSet_0.get(0) );
        
    System.out.println( "Beta: " );
    //Utils.PrintVectorNonZero(plr_agent_0.noReallyGetBeta().getRow(0));
    Utils.PrintVectorNonZero(plr_agent_0.noReallyGetBeta().viewRow(0));
    
    
    System.out.println( "\nGamma: " );
    //Utils.PrintVectorNonZero(plr_agent_0.gamma.getMatrix().getRow(0));
    Utils.PrintVectorNonZero(plr_agent_0.gamma.getMatrix().viewRow(0));
    
  }
  
  public void testLocalGradientFlush() {
    
    
    System.out.println( "\n\n\ntestLocalGradientFlush --------------" );
    
    int categories = 2;
    int numFeatures = 5;
    double lambda = 1.0e-4;
    double learning_rate = 10;

    ArrayList<Vector> trainingSet_0 = new ArrayList<Vector>();
    
    for ( int s = 0; s < 1; s++ ) {
      
      
      Vector input = new RandomAccessSparseVector(numFeatures);
      
      for ( int x = 0; x < numFeatures; x++ ) {
      
        input.set(x, x);
        
      }
      
      trainingSet_0.add(input);

    } // for
    
    ParallelOnlineLogisticRegression plr_agent_0 = new ParallelOnlineLogisticRegression( categories, numFeatures, new L1())
    .lambda(lambda)
    .learningRate(learning_rate)
    .alpha(1 - 1.0e-3);
    
  
    plr_agent_0.train(0, trainingSet_0.get(0) );
    
    System.out.println( "\nGamma: " );
    Utils.PrintVectorNonZero(plr_agent_0.gamma.getMatrix().viewRow(0));
        
    
    plr_agent_0.FlushGamma();

    System.out.println( "Flushing Gamma ...... " );
    
    System.out.println( "\nGamma: " );
    Utils.PrintVector(plr_agent_0.gamma.getMatrix().viewRow(0));
    
    for ( int x = 0; x < numFeatures; x++ ) {
      
      assertEquals( plr_agent_0.gamma.getMatrix().get(0, x), 0.0 );
      
    }
    
  }

  
  
}
