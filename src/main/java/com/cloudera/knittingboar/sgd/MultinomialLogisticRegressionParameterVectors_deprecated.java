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

import java.util.ArrayList;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;

import com.cloudera.knittingboar.utils.Utils;

/**
 * An array of parameter vectors in the form of a Matrix object
 * 
 * A wrapper around another Matrix object to hold the worker's parameter vectors
 * 
 * @author jpatterson
 * 
 */
public class MultinomialLogisticRegressionParameterVectors_deprecated {
  
  protected Matrix gamma; // this is the saved updated gradient we merge at the
                          // super step
  
  private int AccumulatedGradientsCount = 0;
  private int numCategories = 2; // default
  
  public MultinomialLogisticRegressionParameterVectors_deprecated(int numCategories, int numFeatures) {
    
    this.numCategories = numCategories;
    this.gamma = new DenseMatrix(numCategories - 1, numFeatures);
    
  }
  
  public void setMatrix(Matrix m) {
    
    this.gamma = m;
    
  }
  
  public Matrix getMatrix() {
    // close();
    return this.gamma;
  }
  
  public void setCell(int i, int j, double gammaIJ) {
    this.gamma.set(i, j, gammaIJ);
  }
  
  public double getCell(int row, int col) {
    return this.gamma.get(row, col);
  }
  
  public int numFeatures() {
    return this.gamma.numCols();
  }
  
  public int numCategories() {
    return this.numCategories;
  }
  
  private void ClearGradientBuffer() {
    
    this.gamma = this.gamma.like();
    
  }
  
  public void SetupMerge() {
    
    this.AccumulatedGradientsCount = 0;
    this.ClearGradientBuffer();
    
  }
  
  /**
   * TODO: fix loop
   * 
   * @param other_gamma
   */
  public void AccumulateParameterVector(Matrix other_gamma) {
    
//    this.gamma.plus(arg0)
    
    for (int row = 0; row < this.gamma.rowSize(); row++) {
      
      for (int col = 0; col < this.gamma.columnSize(); col++) {
        
        double old_this_val = this.gamma.get(row, col);
        double other_val = other_gamma.get(row, col);
        
        // System.out.println( "Accumulate: " + old_this_val + ", " + other_val
        // );
        
        this.gamma.set(row, col, old_this_val + other_val);
        
        // System.out.println( "new value: " + this.gamma.get(row, col) );
        
      }
      
    }
    
    this.AccumulatedGradientsCount++;
    
  }
/*  
  public void Accumulate(GradientBuffer other_gamma) {
    
    for (int row = 0; row < this.gamma.rowSize(); row++) {
      
      for (int col = 0; col < this.gamma.columnSize(); col++) {
        
        double old_this_val = this.gamma.get(row, col);
        double other_val = other_gamma.getCell(row, col);
        
        this.gamma.set(row, col, old_this_val + other_val);
        
      }
      
    }
    
    this.AccumulatedGradientsCount++;
    
  }
  */
  
  /**
   * TODO: Need to take a look at built in matrix ops here 
   * 
   */
  public void AverageParameterVectors(int denominator) {
    
    for (int row = 0; row < this.gamma.rowSize(); row++) {
      
      for (int col = 0; col < this.gamma.columnSize(); col++) {
        
        double old_this_val = this.gamma.get(row, col);
        // double other_val = other_gamma.getCell(row, col);
        this.gamma.set(row, col, old_this_val / denominator);
        
      }
      
    }
    
  }
  
  /**
   * Copies another GradientBufffer
   * 
   * @param other
   */
/*  public void Copy(MultinomialLogisticRegressionParameterVectors other) {
    
    this.Accumulate(other);
    
  }
  
  public static GradientBuffer Merge(ArrayList<GradientBuffer> gammas) {
    
    int numFeatures = gammas.get(0).numFeatures();
    int numCategories = gammas.get(0).numCategories();
    
    GradientBuffer merged = new GradientBuffer(numCategories, numFeatures);
    
    // accumulate all the gradients into buffers
    
    for (int x = 0; x < gammas.size(); x++) {
      
      merged.Accumulate(gammas.get(x));
      
    }
    
    // calc average
    
    return merged;
    
  }
*/  
  /**
   * Clears all values in matrix back to 0s
   */
  public void Reset() {
    
    for (int row = 0; row < this.gamma.rowSize(); row++) {
      
      for (int col = 0; col < this.gamma.columnSize(); col++) {
        
        this.gamma.set(row, col, 0);
        
      }
      
    }
    
    this.AccumulatedGradientsCount = 0;
    
  }
  
  public void Debug() {
    
    System.out.println("\nGamma: ");
    
    for (int x = 0; x < this.gamma.rowSize(); x++) {
      
      Utils.PrintVectorSectionNonZero(this.gamma.viewRow(x), 3);
      
    }
    
  }
  
  public void DebugRowInGamma(int row) {
    
    System.out.println("\nGamma: ");
    Utils.PrintVectorSectionNonZero(this.gamma.viewRow(row), 3);
    
  }
  
}
