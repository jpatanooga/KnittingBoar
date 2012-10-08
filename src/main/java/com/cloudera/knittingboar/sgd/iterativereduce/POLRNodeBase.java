package com.cloudera.knittingboar.sgd.iterativereduce;

import org.apache.hadoop.conf.Configuration;

public class POLRNodeBase {

  protected Configuration conf = null;
  protected int num_categories = 2;
  protected int FeatureVectorSize = -1;
  protected int BatchSize = 200;
  protected double Lambda = 1.0e-4;
  protected double LearningRate = 50;
  
  String LocalInputSplitPath = "";
  String PredictorLabelNames = "";
  String PredictorVariableTypes = "";
  protected String TargetVariableName = "";
  protected String ColumnHeaderNames = "";
  protected int NumberPasses = 1;
  
  protected int LocalPassCount = 0;
  protected int GlobalPassCount = 0;
  
  protected String RecordFactoryClassname = "";
  
  
  protected String LoadStringConfVarOrException( String ConfVarName, String ExcepMsg ) throws Exception {
    
    if ( null == this.conf.get(ConfVarName) ) {
      throw new Exception(ExcepMsg);
    } else {
      return this.conf.get(ConfVarName);
    }
    
  }

  protected int LoadIntConfVarOrException( String ConfVarName, String ExcepMsg ) throws Exception {
    
    if ( null == this.conf.get(ConfVarName) ) {
      throw new Exception(ExcepMsg);
    } else {
      return this.conf.getInt(ConfVarName, 0);
    }
    
  }  
}
