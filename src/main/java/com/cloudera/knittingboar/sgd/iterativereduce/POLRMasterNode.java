package com.cloudera.knittingboar.sgd.iterativereduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.sgd.L1;

import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradient;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.records.CSVBasedDatasetRecordFactory;
import com.cloudera.knittingboar.records.RCV1RecordFactory;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.cloudera.knittingboar.sgd.GradientBuffer;
import com.cloudera.knittingboar.sgd.POLRModelParameters;
import com.cloudera.knittingboar.sgd.ParallelOnlineLogisticRegression;
import com.cloudera.knittingboar.yarn.appmaster.ApplicationMaster;
import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;

import com.google.common.collect.Lists;

/**
 * TODO - MK does shit here
 * 
 * 
 * 
 * @author jpatterson
 * 
 */
public class POLRMasterNode extends POLRNodeBase implements
    ComputableMaster<ParameterVectorGradientUpdatable> {
  
  private static final Log LOG = LogFactory.getLog(POLRMasterNode.class);

  

  GradientBuffer global_parameter_vector = null;
  //private ArrayList<GlobalParameterVectorUpdateMessage> outgoing_parameter_updates = new ArrayList<GlobalParameterVectorUpdateMessage>();
  //private ArrayList<GradientUpdateMessage> incoming_gradient_updates = new ArrayList<GradientUpdateMessage>();
  
  private int GlobalMaxPassCount = 0;

  // these are only used for saving the model
  public ParallelOnlineLogisticRegression polr = null; 
  public POLRModelParameters polr_modelparams;
  private RecordFactory VectorFactory = null;
  
  
  
  //private UpdateableInt masterTotal;
  
/*  
  public void RecvGradientMessage() {
    
    GradientUpdateMessage rcvd_msg = this.incoming_gradient_updates.remove(0);

    if (rcvd_msg.SrcWorkerPassCount > this.GlobalMaxPassCount) {
      
      this.GlobalMaxPassCount = rcvd_msg.SrcWorkerPassCount; 
      
    }
    
    // accumulate gradient
    this.MergeGradientUpdate( rcvd_msg.gradient );
    
  }  
*/
  
  @Override
  public ParameterVectorGradientUpdatable compute(
      Collection<ParameterVectorGradientUpdatable> workerUpdates,
      Collection<ParameterVectorGradientUpdatable> masterUpdates) {
    
    //int total = 0;
    /*
     * for (UpdateableInt i : workerUpdates) { total += i.get(); }
     * 
     * for (UpdateableInt i : masterUpdates) { total += i.get(); }
     * 
     * //if (masterTotal == null) masterTotal = new UpdateableInt();
     * 
     * masterTotal.set(total); LOG.debug("Current total=" + masterTotal.get() +
     * ", workerUpdates=" + toStrings(workerUpdates) + ", masterUpdates=" +
     * toStrings(masterUpdates));
     */
    
    for (ParameterVectorGradientUpdatable i : workerUpdates) { 
    
      if (i.get().SrcWorkerPassCount > this.GlobalMaxPassCount) {
        
        this.GlobalMaxPassCount = i.get().SrcWorkerPassCount; 
        
      }
      
      // accumulate gradient
      this.global_parameter_vector.AccumulateGradient(i.get().parameter_vector);
      
    }
    
    // now generate the return trip msg
//    master.GenerateGlobalUpdateVector();  
//    GlobalParameterVectorUpdateMessage returned_msg = master.GetNextGlobalUpdateMsgFromQueue();

/*    GlobalParameterVectorUpdateMessage response_msg = new GlobalParameterVectorUpdateMessage( "", this.num_categories, this.FeatureVectorSize );
    response_msg.parameter_vector = this.global_parameter_vector.gamma.clone();
    response_msg.GlobalPassCount = this.GlobalMaxPassCount;
*/
    ParameterVectorGradient gradient_msg = new ParameterVectorGradient();
    gradient_msg.GlobalPassCount = this.GlobalMaxPassCount;
    gradient_msg.parameter_vector = this.global_parameter_vector.getMatrix().clone();
    
    ParameterVectorGradientUpdatable return_msg = new ParameterVectorGradientUpdatable();
    return_msg.set(gradient_msg);

    return return_msg;
  }
 /* 
  private String toStrings(Collection<ParameterVectorGradientUpdatable> c) {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    
      for (UpdateableInt i : c) { sb.append(i.get()).append(", "); }
     
    sb.append("]");
    return sb.toString();
    
  }
  */
  
  @Override
  public ParameterVectorGradientUpdatable getResults() {
    return null;
  }
  
  @Override
  public void setup(Configuration c) {
    
    this.conf = c;
    
    try {
      
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
          "com.cloudera.knittingboar.setup.LearningRate", "50"));
      
      // local input split path
      this.LocalInputSplitPath = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.LocalInputSplitPath",
          "Error loading config: could not load local input split path");
      
      // System.out.println("LoadConfig()");
      
      // maps to either CSV, 20newsgroups, or RCV1
      this.RecordFactoryClassname = LoadStringConfVarOrException(
          "com.cloudera.knittingboar.setup.RecordFactoryClassname",
          "Error loading config: could not load RecordFactory classname");
      
      if (this.RecordFactoryClassname.equals(RecordFactory.CSV_RECORDFACTORY)) {
        
        // so load the CSV specific stuff ----------
        System.out.println( "----- Loading CSV RecordFactory Specific Stuff -------" );
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
      
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( ">> Error loading conf!" );
    }
    
    this.SetupPOLR();
    
  } // setup()
  
  
  
  
  public void SetupPOLR() {
    
    System.out.println( "SetupOLR: " + this.num_categories + ", " + this.FeatureVectorSize );
    
    
    this.global_parameter_vector = new GradientBuffer( this.num_categories, this.FeatureVectorSize );
    
    
    String[] predictor_label_names = this.PredictorLabelNames.split(",");

    String[] variable_types = this.PredictorVariableTypes.split(",");
    
    polr_modelparams = new POLRModelParameters();
    polr_modelparams.setTargetVariable( this.TargetVariableName ); //getStringArgument(cmdLine, target));
    polr_modelparams.setNumFeatures( this.FeatureVectorSize );
    polr_modelparams.setUseBias(true); //!getBooleanArgument(cmdLine, noBias));
    
    List<String> typeList = Lists.newArrayList();
    for ( int x = 0; x < variable_types.length; x++ ) {
      typeList.add( variable_types[x] );
    }

    List<String> predictorList = Lists.newArrayList();
    for ( int x = 0; x < predictor_label_names.length; x++ ) {
      predictorList.add( predictor_label_names[x] );
    }    
    
    polr_modelparams.setTypeMap(predictorList, typeList);
    polr_modelparams.setLambda( this.Lambda ); // based on defaults - match command line
    polr_modelparams.setLearningRate( this.LearningRate ); // based on defaults - match command line
    
    // setup record factory stuff here ---------

    if (RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY.equals(this.RecordFactoryClassname)) {

      this.VectorFactory = new TwentyNewsgroupsRecordFactory("\t");
      
    } else if (RecordFactory.RCV1_RECORDFACTORY.equals(this.RecordFactoryClassname)) {
      
      this.VectorFactory = new RCV1RecordFactory();
      
    } else {
      
      // need to rethink this
      
      this.VectorFactory = new CSVBasedDatasetRecordFactory(this.TargetVariableName, polr_modelparams.getTypeMap() );
      
      ((CSVBasedDatasetRecordFactory)this.VectorFactory).firstLine( this.ColumnHeaderNames );
      
    }
    
    
    polr_modelparams.setTargetCategories( this.VectorFactory.getTargetCategories() );
    
    // ----- this normally is generated from the POLRModelParams ------
    
    this.polr = new ParallelOnlineLogisticRegression(this.num_categories, this.FeatureVectorSize, new L1())
    .alpha(1).stepOffset(1000)
    .decayExponent(0.9) 
    .lambda(this.Lambda)
    .learningRate( this.LearningRate );   
    
    polr_modelparams.setPOLR(polr);    
    //this.bSetup = true;

  }

  @Override
  public void complete(DataOutputStream out) throws IOException {
    // TODO Auto-generated method stub
    System.out.println( "master::complete " );
    System.out.println( "complete-ms:" + System.currentTimeMillis() );

    try {
      this.polr_modelparams.saveTo(out);
    } catch (Exception ex) {
      throw new IOException("Unable to save model", ex);
    }
  }  
  
  public static void main(String[] args) throws Exception {
    POLRMasterNode pmn = new POLRMasterNode();
    ApplicationMaster<ParameterVectorGradientUpdatable> am = new ApplicationMaster<ParameterVectorGradientUpdatable>(
        pmn, ParameterVectorGradientUpdatable.class);
        
    ToolRunner.run(am, args);
  }
}
