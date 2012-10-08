package com.cloudera.knittingboar.sgd;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelDissector;
//import org.apache.mahout.classifier.sgd.RecordFactory;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.GradientUpdateMessage;
import com.cloudera.knittingboar.metrics.POLRMetrics;
import com.cloudera.knittingboar.records.CSVBasedDatasetRecordFactory;
import com.cloudera.knittingboar.records.RCV1RecordFactory;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
/*
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
*/

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

/**
 * Primary controller of the ParallelOnlineLogisticRegression class
 * Allows us to configure and drive the training process
 * 
 * - does the work on the subset of the training data
 * 
 * - simulates a worker node for SGD algo development, not the actual yarn-based driver
 * 
 * @author jpatterson
 *
 */
public class POLRWorkerDriver extends POLRBaseDriver {

  public ParallelOnlineLogisticRegression polr = null; //lmp.createRegression();
  public POLRModelParameters polr_modelparams;
  
  public String internalID = "0";
  private RecordFactory VectorFactory = null;
  InputRecordsSplit input_split = null;
  
  ModelDissector md = new ModelDissector();
 
  // basic stats tracking
  POLRMetrics metrics = new POLRMetrics();
  
  double averageLineCount = 0.0;
  int k = 0;
  double step = 0.0;
  int[] bumps = new int[]{1, 2, 5};
  double lineCount = 0;

  public POLRWorkerDriver() {
    
  }
  
  public RecordFactory getRecordFactory() {
    return this.VectorFactory;
  }
  
  /**
   * Needs to update the parameter vector from the newly minted global parameter vector
   * and then clear out the gradient buffer
   * 
   * @param msg
   */
  public void ProcessIncomingParameterVectorMessage( GlobalParameterVectorUpdateMessage msg) {
    
    this.RecvMasterParamVector(msg.parameter_vector);
    
    // update global count
    this.GlobalPassCount = msg.GlobalPassCount;
    
    this.polr.FlushGamma();
  }
  
  public void setupInputSplit( InputRecordsSplit split ) {
    
    this.input_split = split;
    
  }
 

  
  
  /**
   * called after conf vars are loaded
   */
  public void Setup() {
    
    // do splitting strings into arrays here...
    String[] predictor_label_names = this.PredictorLabelNames.split(",");
    String[] variable_types = this.PredictorVariableTypes.split(",");
    
    polr_modelparams = new POLRModelParameters();
    polr_modelparams.setTargetVariable( this.TargetVariableName ); 
    polr_modelparams.setNumFeatures( this.FeatureVectorSize );
    polr_modelparams.setUseBias(true); 
    
    List<String> typeList = Lists.newArrayList();
    for ( int x = 0; x < variable_types.length; x++ ) {
      typeList.add( variable_types[x] );
    }

    List<String> predictorList = Lists.newArrayList();
    for ( int x = 0; x < predictor_label_names.length; x++ ) {
      predictorList.add( predictor_label_names[x] );
    }    
    
    // where do these come from?
    polr_modelparams.setTypeMap(predictorList, typeList);
    polr_modelparams.setLambda( this.Lambda ); // based on defaults - match command line
    polr_modelparams.setLearningRate( this.LearningRate ); // based on defaults - match command line
    
    // setup record factory stuff here ---------

    if (RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY.equals(this.RecordFactoryClassname)) {

      this.VectorFactory = new TwentyNewsgroupsRecordFactory("\t");
      
    } else if (RecordFactory.RCV1_RECORDFACTORY.equals(this.RecordFactoryClassname)) {
      
      this.VectorFactory = new RCV1RecordFactory();
      
    } else {
      
      // it defaults to the CSV record factor, but a custom one
      
      this.VectorFactory = new CSVBasedDatasetRecordFactory(this.TargetVariableName, polr_modelparams.getTypeMap() );
      
      ((CSVBasedDatasetRecordFactory)this.VectorFactory).firstLine( this.ColumnHeaderNames );
      
      
    }
        
    polr_modelparams.setTargetCategories( this.VectorFactory.getTargetCategories() );
    
    // ----- this normally is generated from the POLRModelParams ------
    
    this.polr = new ParallelOnlineLogisticRegression(this.num_categories, this.FeatureVectorSize, new L1())
    .alpha(1).stepOffset(1000)
    .decayExponent(0.9) 
    .lambda(this.Lambda)
    .learningRate(this.LearningRate);   
    
    polr_modelparams.setPOLR(polr);
    
    this.bSetup = true;
  }
  
  
  /**
   * 
   * Main running method for algorithm wrt SlaveNode
   * 1. run the next batch
   * 2. get the gradient
   * 3. send the gradient to the master node
   * 4. process up to N more instances while waiting on parameter_vector update
   * 5. [ async ] update the parameter_vector when the response gets back to us
   * 6. apply any gradient updates to catch us up
   */
  public void Run() {
    
    boolean bRun = true;
    
    while (bRun) {
      
      // run next batch
      try {
        this.RunNextTrainingBatch();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      
      // send gradient out to mstr_node

      this.BroadcastGradientUpdateToMaster();
      
      
      
      
      
    }
    
  }
  
  private void BroadcastGradientUpdateToMaster() {
    
    // do stuff, talk to YARN, blah blah blah
    
  }
  
  /**
   * When the messaging systme gets the parameter vector update back, we call this method
   * 1. udpate the local p-vector
   * 2. process an latent gradient -> update
   * 3. lets the training cycle continue on w/o further blocking
   * 
   */
  private void RecvMasterParamVector( Matrix beta ) {
    
    this.polr.SetBeta(beta);
    
  }
  
  
  /**
   * Runs the next training batch to prep the gamma buffer to send to the mstr_node
   * 
   * TODO: need to provide stats, group measurements into struct
   * 
   * @throws Exception 
   * @throws IOException 
   */
  public boolean RunNextTrainingBatch() throws IOException, Exception {
    
    Text value = new Text();
    long batch_vec_factory_time = 0;
    
    if (this.LocalPassCount > this.GlobalPassCount) {
      // we need to sit this one out
      System.out.println( "Worker " + this.internalID + " is ahead of global pass count [" + this.LocalPassCount + ":" + this.GlobalPassCount + "] "  );
      return true;
    }
    
    if (this.LocalPassCount >= this.NumberPasses) {
      // learning is done, terminate
      System.out.println( "Worker " + this.internalID + " is done [" + this.LocalPassCount + ":" + this.GlobalPassCount + "] "  );
      return false;
    }
    
    for (int x = 0; x < this.BatchSize; x++ ) {
      
      if ( this.input_split.next(value)) {
        
        long startTime = System.currentTimeMillis();

        Vector v = new RandomAccessSparseVector(this.FeatureVectorSize);
        int actual = this.VectorFactory.processLine(value.toString(), v);

        long endTime = System.currentTimeMillis();

        batch_vec_factory_time += (endTime - startTime);
        
//        String ng = this.VectorFactory.GetClassnameByID(actual); //.GetNewsgroupNameByID( actual );
        
        // calc stats ---------
        
        double mu = Math.min(k + 1, 200);
        double ll = this.polr.logLikelihood(actual, v);  

        metrics.AvgLogLikelihood = metrics.AvgLogLikelihood + (ll - metrics.AvgLogLikelihood) / mu; 

        Vector p = new DenseVector(20);
        this.polr.classifyFull(p, v);
        int estimated = p.maxValueIndex();
        int correct = (estimated == actual? 1 : 0);
        metrics.AvgCorrect = metrics.AvgCorrect + (correct - metrics.AvgCorrect) / mu;         
        this.polr.train(actual, v);
        
        k++;
        if (x == this.BatchSize - 1) {

          System.out.printf("Worker %s:\t Trained Recs: %10d, loglikelihood: %10.3f, AvgLL: %10.3f, Percent Correct: %10.2f, VF: %d\n",
              this.internalID, k, ll, metrics.AvgLogLikelihood, metrics.AvgCorrect * 100, batch_vec_factory_time);
          
        }
        
        this.polr.close();                  
      
      }  else {
        
        this.LocalPassCount++;
        this.input_split.ResetToStartOfSplit();
        // nothing else to process in split!
        break;
        
      } // if
      
      
    } // for the batch size
    
    return true;
    
  }
  
  /**
   * Generates update message
   * 
   * @return
   */
  public GradientUpdateMessage GenerateUpdateMessage() {
    
    GradientUpdateMessage msg0 = new GradientUpdateMessage(this.getHostAddress(), this.polr.gamma );
    msg0.SrcWorkerPassCount = this.LocalPassCount;
    return msg0;
    
  }

  
  /**
   * TODO: break this down, review how it fits into where we're going
   */
  public void PrintModelStats() {
/*    
    System.out.println( "Elements in csvVectorFactory dictionary ---" );
    for (String v : this.VectorFactory.getTraceDictionary().keySet()) {

      System.out.println( "> " + v );
    }    
    
    System.out.printf(Locale.ENGLISH, "Features: %d\n", polr_modelparams.getNumFeatures());
    System.out.printf(Locale.ENGLISH, "Target Variable: %s ~ ", polr_modelparams.getTargetVariable());
    String sep = "";
    for (String v : csvVectorFactory.getTraceDictionary().keySet()) {
      double weight = predictorWeight(polr, 0, csvVectorFactory, v);
      if (weight != 0) {
        System.out.printf(Locale.ENGLISH, "%s%.3f*%s", sep, weight, v);
        sep = " + ";
      }
    }
    System.out.printf("\n");
    //model = polr;
    for (int row = 0; row < polr.getBeta().numRows(); row++) {
      for (String key : csvVectorFactory.getTraceDictionary().keySet()) {
        double weight = predictorWeight(polr, row, csvVectorFactory, key);
        if (weight != 0) {
          System.out.printf(Locale.ENGLISH, "%20s %.5f\n", key, weight);
        }
      }
      for (int column = 0; column < polr.getBeta().numCols(); column++) {
        System.out.printf(Locale.ENGLISH, "%15.9f ", polr.getBeta().get(row, column));
      }
      System.out.println();
    }    
*/    
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
  
  /**
   * NOTE: This should only be used for durability purposes in checkpointing the workers
   * 
   * @param path
   * @throws IOException
   */
  public void Load( String path ) throws IOException {
    
    InputStream in = new FileInputStream(path);
    try {
      polr_modelparams.loadFrom(in);
    } finally {
      Closeables.closeQuietly(in);
    }    
    
  }
  
  public void Debug() throws IOException {
    
    System.out.println( "POLRWorkerDriver --------------------------- " );
    
    System.out.println( "> Num Categories: " + this.num_categories );
    System.out.println( "> FeatureVecSize: " + this.FeatureVectorSize );
    
    this.polr_modelparams.Debug();
    
    
  }
  
  
  
}
