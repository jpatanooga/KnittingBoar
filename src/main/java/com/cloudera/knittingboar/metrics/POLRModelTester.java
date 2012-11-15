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

package com.cloudera.knittingboar.metrics;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelDissector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.records.CSVBasedDatasetRecordFactory;
import com.cloudera.knittingboar.records.RCV1RecordFactory;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.cloudera.knittingboar.sgd.POLRBaseDriver;
import com.cloudera.knittingboar.sgd.POLRModelParameters;
import com.cloudera.knittingboar.sgd.ParallelOnlineLogisticRegression;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class POLRModelTester extends POLRBaseDriver {
  
  public ParallelOnlineLogisticRegression polr = null; // lmp.createRegression();
  
  public POLRModelParameters polr_modelparams;
  
  // private static int passes;
  private static boolean scores = false;
  public String internalID = "TEST";
  
  private RecordFactory VectorFactory = null;
  
  InputRecordsSplit input_split = null;
  
  // TODO: dissect, use this
  ModelDissector md = new ModelDissector();
  
  // basic stats tracking
  POLRMetrics metrics = new POLRMetrics();
  
  // double averageLL = 0.0;
  // double averageCorrect = 0.0;
  double averageLineCount = 0.0;
  int k = 0;
  double step = 0.0;
  int[] bumps = new int[] {1, 2, 5};
  double lineCount = 0;
  
  public void SetCore(ParallelOnlineLogisticRegression plr,
      POLRModelParameters params, RecordFactory fac) {
    
    this.polr = plr;
    this.polr_modelparams = params;
    
    this.VectorFactory = fac;
    
    this.num_categories = this.polr_modelparams.getMaxTargetCategories();
    this.FeatureVectorSize = this.polr_modelparams.getNumFeatures();
    
    this.BatchSize = 10000;
  }
  
  public void Setup() {
    
    // do splitting strings into arrays here...
    
    this.num_categories = -1; // polr_modelparams.getMaxTargetCategories();
    this.FeatureVectorSize = -1; // polr_modelparams.getNumFeatures();
    
    this.BatchSize = 10000;
    
    // setup record factory stuff here ---------
    
    if (RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY
        .equals(this.RecordFactoryClassname)) {
      
      this.VectorFactory = new TwentyNewsgroupsRecordFactory("\t");
      // this.VectorFactory.setClassSplitString("\t");
      
      // System.out.println(
      // "POLRModelTester: TwentyNewsgroupsRecordFactory\n\n" );
      
    } else if (RecordFactory.RCV1_RECORDFACTORY
        .equals(this.RecordFactoryClassname)) {
      
      this.VectorFactory = new RCV1RecordFactory();
      
    } else {
      
      System.out.println("POLRModelTester: CSV is broken!!\n\n\n");
      this.VectorFactory = new CSVBasedDatasetRecordFactory(
          this.TargetVariableName, polr_modelparams.getTypeMap());
      
      ((CSVBasedDatasetRecordFactory) this.VectorFactory)
          .firstLine(this.ColumnHeaderNames);
      
    }
    
    // this.bSetup = true;
  }
  
  /**
   * Runs the next training batch to prep the gamma buffer to send to the
   * mstr_node
   * 
   * TODO: need to provide stats, group measurements into struct
   * 
   * @throws Exception
   * @throws IOException
   */
  public void RunThroughTestRecords() throws IOException, Exception {
    
    Text value = new Text();
    long batch_vec_factory_time = 0;
    k = 0;
    int num_correct = 0;
    
    for (int x = 0; x < this.BatchSize; x++) {
      
      if (this.input_split.next(value)) {
        
        long startTime = System.currentTimeMillis();
        
        Vector v = new RandomAccessSparseVector(this.FeatureVectorSize);
        int actual = this.VectorFactory.processLine(value.toString(), v);
        
        long endTime = System.currentTimeMillis();
        
        // System.out.println("That took " + (endTime - startTime) +
        // " milliseconds");
        batch_vec_factory_time += (endTime - startTime);
        
        String ng = this.VectorFactory.GetClassnameByID(actual); // .GetNewsgroupNameByID(
                                                                 // actual );
        
        // calc stats ---------
        
        double mu = Math.min(k + 1, 200);
        double ll = this.polr.logLikelihood(actual, v);
        
        if (Double.isNaN(ll)) {

          /*
           * System.out.println(" --------- NaN -----------");
           * 
           * System.out.println( "k: " + k ); System.out.println( "ll: " + ll );
           * System.out.println( "mu: " + mu );
           */
          // return;
        } else {
          
          metrics.AvgLogLikelihood = metrics.AvgLogLikelihood
              + (ll - metrics.AvgLogLikelihood) / mu;
          
        }
        
        Vector p = new DenseVector(20);
        this.polr.classifyFull(p, v);
        int estimated = p.maxValueIndex();
        
        int correct = (estimated == actual ? 1 : 0);
        if (estimated == actual) {
          num_correct++;
        }
        // averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
        metrics.AvgCorrect = metrics.AvgCorrect
            + (correct - metrics.AvgCorrect) / mu;
        
        // this.polr.train(actual, v);
        
        k++;
        // if (x == this.BatchSize - 1) {
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
        
        if (k % (bump * scale) == 0) {
          step += 0.25;
          
          System.out
              .printf(
                  "Worker %s:\t Trained Recs: %10d, numCorrect: %d, AvgLL: %10.3f, Percent Correct: %10.2f, VF: %d\n",
                  this.internalID, k, num_correct, metrics.AvgLogLikelihood,
                  metrics.AvgCorrect * 100, batch_vec_factory_time);
          
        }
        
        this.polr.close();
        
      } else {
        
        // nothing else to process in split!
        break;
        
      } // if
      
    } // for the number of passes in the run
    
  }
  
  /**
   * NOTE: This should only be used for durability purposes in checkpointing the
   * workers
   * 
   * @param path
   * @throws IOException
   */
  public void Load(String path) throws IOException {
    
    InputStream in = new FileInputStream(path);
    try {
      polr_modelparams = POLRModelParameters.loadFrom(in);
      System.out.println("> tester: model loaded");
    } finally {
      Closeables.closeQuietly(in);
    }
    
    // System.out.println( "POLRModelTester > num categories is hardcoded to 2"
    // );
    this.num_categories = polr_modelparams.getMaxTargetCategories();
    this.FeatureVectorSize = polr_modelparams.getNumFeatures();
    
    /*
     * this.polr = new ParallelOnlineLogisticRegression(this.num_categories,
     * this.FeatureVectorSize, new L1()) .alpha(1).stepOffset(1000)
     * .decayExponent(0.9) .lambda(3.0e-5) .learningRate(20);
     */
    this.polr = polr_modelparams.getPOLR();
    
    // System.out.println(")))))))))) Learning rate: " + this.Lambda);
    
  }
  
  public void setupInputSplit(InputRecordsSplit split) {
    
    this.input_split = split;
    
  }
  
  public void Debug() throws IOException {
    
    System.out.println("POLRModelTester --------------------------- ");
    
    System.out.println("> Num Categories: " + this.num_categories);
    System.out.println("> FeatureVecSize: " + this.FeatureVectorSize);
    
    this.polr_modelparams.Debug();
    
  }
  
}
