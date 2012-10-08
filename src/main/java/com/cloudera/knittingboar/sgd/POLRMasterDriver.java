package com.cloudera.knittingboar.sgd;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.sgd.L1;

import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;
import com.cloudera.knittingboar.records.CSVBasedDatasetRecordFactory;
import com.cloudera.knittingboar.records.RCV1RecordFactory;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.cloudera.knittingboar.utils.Utils;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.mongodb.util.Util;


/**
 * Runs as the master node in this parallel iterative algorithm
 * - collects gradient updates from slave nodes
 * - updates its locally held global parameter vector
 * - sends a copy back to the slave node to update its own parameter vector
 * - manages execution of the whole POLR process
 * 
 * - this is the basic simulated version of the POLR master
 * 
 * @author jpatterson
 *
 */
public class POLRMasterDriver extends POLRBaseDriver {

  GradientBuffer global_parameter_vector = null;
  private ArrayList<GlobalParameterVectorUpdateMessage> outgoing_parameter_updates = new ArrayList<GlobalParameterVectorUpdateMessage>();
  private ArrayList<GradientUpdateMessage> incoming_gradient_updates = new ArrayList<GradientUpdateMessage>();
  
  private int GlobalMaxPassCount = 0;

  // these are only used for saving the model
  public ParallelOnlineLogisticRegression polr = null; 
  public POLRModelParameters polr_modelparams;
  private RecordFactory VectorFactory = null;
  
  public POLRMasterDriver() {
    
    
    
  }
   
  /**
   * Take the newly loaded config junk and setup the local data structures
   * 
   */
  public void Setup() {
    
    
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
    this.bSetup = true;

  }
  
  
  public void Start() {
    
    this.bRunning = true;
    
  }
  
  public void Stop() {
  
    this.bRunning = false;
    
  }
  
  public void AddIncomingGradientMessageToQueue( GradientUpdateMessage e ) {
    
    this.incoming_gradient_updates.add(e);
    
  }
  
  public void RecvGradientMessage() {
    
    GradientUpdateMessage rcvd_msg = this.incoming_gradient_updates.remove(0);

    if (rcvd_msg.SrcWorkerPassCount > this.GlobalMaxPassCount) {
      
      this.GlobalMaxPassCount = rcvd_msg.SrcWorkerPassCount; 
      
    }
    
    // accumulate gradient
    this.MergeGradientUpdate( rcvd_msg.gradient );
    
  }
  
  public void GenerateGlobalUpdateVector() {
    
    // post message back to sender async
    GlobalParameterVectorUpdateMessage response_msg = new GlobalParameterVectorUpdateMessage( "", this.num_categories, this.FeatureVectorSize );
    response_msg.parameter_vector = this.global_parameter_vector.gamma.clone();
    response_msg.GlobalPassCount = this.GlobalMaxPassCount;
    
    this.SendParameterUpdateMessage(response_msg);
    
  }
  
  public void SendParameterUpdateMessage( GlobalParameterVectorUpdateMessage msg) {
    
    this.outgoing_parameter_updates.add( msg );
    
  }
  
  /**
   * Used to collect gradient updates into buffer
   * 
   * @param incoming_buffer
   */
  private void MergeGradientUpdate( GradientBuffer incoming_buffer ) {
    
    this.global_parameter_vector.Accumulate(incoming_buffer);
    
  }
  
  /**
   * this is mostly for debug
   * 
   * @return
   */
  public GlobalParameterVectorUpdateMessage GetNextGlobalUpdateMsgFromQueue() {
    
    return this.outgoing_parameter_updates.remove(0);
    
  }
  
  
  /**
   * TODO: how does this work differently than the other save method?
   * 
   * 
   * NOTE: This should only be used for durability purposes in checkpointing the workers
   * 
   * 
   */
  public void SaveModelLocally( String outputFile ) throws Exception {
    
    this.polr.SetBeta(this.global_parameter_vector.gamma);
    
    OutputStream modelOutput = new FileOutputStream(outputFile);
    try {
      polr_modelparams.saveTo(modelOutput);
    } finally {
      Closeables.closeQuietly(modelOutput);
    }
    
  }  
  

  /**
   * [ needs to be checked ]
   * 
   * NOTE: This should only be used for durability purposes in checkpointing the workers
   * 
   * @param outputFilename
   * @param conf
   * @throws Exception 
   */
  public void SaveModelToHDFS( String outputFilename, Configuration conf ) throws Exception {
    
    Path path = new Path( outputFilename );
    FileSystem fs = path.getFileSystem(conf);
    FSDataOutputStream modelHDFSOutput = fs.create(path, true);
    
    try {
      polr_modelparams.saveTo(modelHDFSOutput);
    } finally {
      modelHDFSOutput.close();
    }    
    
  } 
  
  
  public void Debug() throws IOException {
    
    System.out.println( "POLRMasterDriver --------------------------- " );
    
    System.out.println( "> Num Categories: " + this.num_categories );
    System.out.println( "> FeatureVecSize: " + this.FeatureVectorSize );
    
    this.polr_modelparams.Debug();
    
    
  }

  
  
  
}
