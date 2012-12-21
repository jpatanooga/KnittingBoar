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

import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.records.RecordFactory;
import com.google.common.annotations.VisibleForTesting;

/**
 * Base class for both the master and the worker process for POLR
 * 
 * Basic usage:
 * 
 * - either have a configuration variable loaded from YARN params/data
 * 
 * [or]
 * 
 * - create a debug/test/sythetic Configuration object and pass that in via
 * .debug_setConf( Configuration )
 * 
 * - LoadConfigVarsLocally() is called to pull data from conf
 * 
 * - Setup() is called to build any ML/SGD specific stuff based on the
 * configuration
 * 
 * @author jpatterson
 * 
 */
public abstract class POLRBaseDriver {
  
  boolean bConfLoaded = false;
  boolean bSetup = false;
  boolean bRunning = false;
  
  private Configuration conf = null;
  protected int num_categories = 2;
  protected int FeatureVectorSize = -1;
  protected int BatchSize = 200;
  protected double Lambda = 1.0e-4;
  protected double LearningRate = 10;
  
  String LocalInputSplitPath = "";
  String PredictorLabelNames = "";
  String PredictorVariableTypes = "";
  protected String TargetVariableName = "";
  protected String ColumnHeaderNames = "";
  protected int NumberPasses = 1;
  
  protected int LocalPassCount = 0;
  protected int GlobalPassCount = 0;
  
  protected String RecordFactoryClassname = "";
  
  /**
   * used with unit tests to pre-set a Configuration
   * 
   * 
   * @param c
   */
  @VisibleForTesting
  public void setConf(Configuration c) {
    
    this.conf = c;
    
  }
  
  public Configuration getConf() {
    
    return this.conf;
    
  }
  
  /**
   * Loads the config from [HDFS / JobConf]
   * 
   * 
   * NOTES - the mechanics of Configuration may be different in this context -
   * where does Configuration typically get its info from onload?
   * 
   * @throws Exception
   * 
   */
  public void LoadConfigVarsLocally() throws Exception {
    
    // figure out how many features we need
    
    this.bConfLoaded = false;
    
    // this is hard set with LR to 2 classes
    this.num_categories = this.conf.getInt(
        "com.cloudera.knittingboar.setup.numCategories", 2);
    
    // feature vector size
    this.FeatureVectorSize = LoadIntConfVarOrException(
        "com.cloudera.knittingboar.setup.FeatureVectorSize",
        "Error loading config: could not load feature vector size");
    
    // feature vector size
    this.BatchSize = this.conf.getInt(
        "com.cloudera.knittingboar.setup.BatchSize", 200);
    
    this.NumberPasses = this.conf.getInt(
        "com.cloudera.knittingboar.setup.NumberPasses", 1);
    
    // protected double Lambda = 1.0e-4;
    this.Lambda = Double.parseDouble(this.conf.get(
        "com.cloudera.knittingboar.setup.Lambda", "1.0e-4"));
    
    // protected double LearningRate = 50;
    this.LearningRate = Double.parseDouble(this.conf.get(
        "com.cloudera.knittingboar.setup.LearningRate", "10"));
    
    // local input split path
//    this.LocalInputSplitPath = LoadStringConfVarOrException(
//        "com.cloudera.knittingboar.setup.LocalInputSplitPath",
//        "Error loading config: could not load local input split path");
    
    // System.out.println("LoadConfig()");
    
    // maps to either CSV, 20newsgroups, or RCV1
    this.RecordFactoryClassname = LoadStringConfVarOrException(
        "com.cloudera.knittingboar.setup.RecordFactoryClassname",
        "Error loading config: could not load RecordFactory classname");
    
    if (this.RecordFactoryClassname.equals(RecordFactory.CSV_RECORDFACTORY)) {
      
      // so load the CSV specific stuff ----------
      
      // predictor label names
      this.PredictorLabelNames = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.PredictorLabelNames",
          "Error loading config: could not load predictor label names");
      
      // predictor var types
      this.PredictorVariableTypes = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.PredictorVariableTypes",
          "Error loading config: could not load predictor variable types");
      
      // target variables
      this.TargetVariableName = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.TargetVariableName",
          "Error loading config: Target Variable Name");
      
      // column header names
      this.ColumnHeaderNames = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.ColumnHeaderNames",
          "Error loading config: Column Header Names");
      
      // System.out.println("LoadConfig(): " + this.ColumnHeaderNames);
      
    }
    
    this.bConfLoaded = true;
    
  }
  
  public int GetCurrentLocalPassCount() {
    return this.LocalPassCount;
  }
  
  public void IncGlobalPassCount() {
    this.GlobalPassCount++;
  }
  
  private String LoadStringConfVarOrException(String ConfVarName,
      String ExcepMsg) throws Exception {
    
    if (null == this.conf.get(ConfVarName)) {
      throw new Exception(ExcepMsg);
    } else {
      return this.conf.get(ConfVarName);
    }
    
  }
  
  private int LoadIntConfVarOrException(String ConfVarName, String ExcepMsg)
      throws Exception {
    
    if (null == this.conf.get(ConfVarName)) {
      throw new Exception(ExcepMsg);
    } else {
      return this.conf.getInt(ConfVarName, 0);
    }
    
  }
  
  /**
   * After the config is loaded, the driver then sets up its local data
   * stuctures
   */
  public void Setup() {}
  
  public void DebugPrintConfig() {
    
    System.out.println("");
    
    // this is hard set with LR to 2 classes
    System.out.println("Num Categories: " + this.num_categories);
    
    // feature vector size
    System.out.println("Feature Vector Size: " + this.FeatureVectorSize);
    
    // local input split path
    System.out.println("Local Input Split Path: " + this.LocalInputSplitPath);
    
    // predictor label names
    System.out.println("Predictor Label Names: " + this.PredictorLabelNames);
    
    // predictor var types
    System.out.println("Predictor Variable Types: "
        + this.PredictorVariableTypes);
    
    // target variables
    System.out.println("Target Variable Name: " + this.TargetVariableName);
    
    // column header names
    System.out.println("Column Header Names: " + this.ColumnHeaderNames);
    
    System.out.println("Lambda: " + this.Lambda);
    
    System.out.println("LearningRate: " + this.LearningRate);
    
    System.out.println("NumberPasses: " + this.NumberPasses);
    
  }
  
  /**
   * based control method
   */
  public void Run() {}
  
  /**
   * based control method
   */
  public void Start() {}
  
  /**
   * based control method
   */
  public void Stop() {}
  
  // TODO: fix!
  public String getHostAddress() {
    return "127.0..1";
  }
  
}
