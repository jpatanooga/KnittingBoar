package com.cloudera.knittingboar.sgd.iterativereduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.UniformPrior;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradient;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.metrics.POLRMetrics;
import com.cloudera.knittingboar.records.CSVBasedDatasetRecordFactory;
import com.cloudera.knittingboar.records.RCV1RecordFactory;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.cloudera.knittingboar.sgd.GradientBuffer;
import com.cloudera.knittingboar.sgd.POLRModelParameters;
import com.cloudera.knittingboar.sgd.ParallelOnlineLogisticRegression;
//import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;

import com.cloudera.iterativereduce.ComputableWorker;
import com.cloudera.iterativereduce.yarn.appworker.ApplicationWorker;

import com.cloudera.iterativereduce.io.RecordParser;
import com.cloudera.iterativereduce.io.TextRecordParser;
import com.google.common.collect.Lists;


/**
 * TODO
 * -  Need a way to provide a record reader
 *    - need a way to tell the record reader how many records to do per batch
 * 
 * - how does configure() integrate?
 * 
 * - 
 * 
 * 
 * 
 * @author jpatterson
 *
 */


public class POLRWorkerNode extends POLRNodeBase implements ComputableWorker<ParameterVectorGradientUpdatable> {

  private static final Log LOG = LogFactory.getLog(POLRWorkerNode.class);
  
  int masterTotal = 0;
  //UpdateableInt workerTotal;
  
  
  
  
  
  
  

  public ParallelOnlineLogisticRegression polr = null; //lmp.createRegression();
  public POLRModelParameters polr_modelparams;
  
  public String internalID = "0";
  private RecordFactory VectorFactory = null;
  
  // old way
  //InputRecordsSplit input_split = null;
  
  // new way
  //private HDFSLineParser lineParser = null;
  private TextRecordParser lineParser = null;
  
 
  // basic stats tracking
  POLRMetrics metrics = new POLRMetrics();
  
  double averageLineCount = 0.0;
  int k = 0;
  double step = 0.0;
  int[] bumps = new int[]{1, 2, 5};
  double lineCount = 0;  
  
  /**
   * Needs to update the parameter vector from the newly minted global parameter vector
   * and then clear out the gradient buffer
   * 
   * @param msg
   */
/*  public void ProcessIncomingParameterVectorMessage( GlobalParameterVectorUpdateMessage msg) {
    
    //this.RecvMasterParamVector(msg.parameter_vector);
    this.polr.SetBeta(msg.parameter_vector);
    
    // update global count
    this.GlobalPassCount = msg.GlobalPassCount;
    
    this.polr.FlushGamma();
  }  
  */
  /*
  public GradientUpdateMessage GenerateParamVectorUpdateMessage() {
    
    GradientBuffer gb = new GradientBuffer( this.num_categories, this.FeatureVectorSize );
    gb.setMatrix(this.polr.getBeta());
    
    GradientUpdateMessage msg0 = new GradientUpdateMessage(this.getHostAddress(), gb );
    msg0.SrcWorkerPassCount = this.LocalPassCount;
    return msg0;
    
  }
   */
  /**
   * Sends a full copy of the multinomial logistic regression array of parameter vectors to the master
   * - this method plugs the local parameter vector into the message
   */
  public ParameterVectorGradient GenerateUpdate() {
    
    
    
    ParameterVectorGradient gradient = new ParameterVectorGradient();
    gradient.parameter_vector = this.polr.getBeta().clone(); //this.polr.getGamma().getMatrix().clone();
    gradient.SrcWorkerPassCount = this.LocalPassCount;
    
/*    try {
      System.out.println( "GenerateUpdate >> " + gradient.parameter_vector.get(0, 0) + ", Len: " + gradient.Serialize().length);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
*/    
    return gradient;
    
  }  
  
  
  @Override
  public ParameterVectorGradientUpdatable compute() {
    int total = 0;
/*    
    for(String s : records) {
      Integer i = Integer.parseInt(s);
      total += i;
    }
    
    
    
    //masterTotal = total / 10;
    
    //if (workerTotal == null)
      workerTotal = new UpdateableInt();
    
    workerTotal.set(masterTotal + total);
    LOG.debug("Current total=" + workerTotal.get() 
        + ", records=" + records.toString());
    */
    
    //HDFSLineParser input_parser = (HDFSLineParser) this.getRecordParser();
    
    
    // ------------- process the next batch -----------
    
    Text value = new Text();
    long batch_vec_factory_time = 0;
/*    
    if (this.LocalPassCount > this.GlobalPassCount) {
      // we need to sit this one out
      System.out.println( "Worker " + this.internalID + " is ahead of global pass count [" + this.LocalPassCount + ":" + this.GlobalPassCount + "] "  );
      return null;
    }
    
    if (this.LocalPassCount >= this.NumberPasses) {
      // learning is done, terminate
      System.out.println( "Worker " + this.internalID + " is done [" + this.LocalPassCount + ":" + this.GlobalPassCount + "] "  );
      return null;
    }
*/
    //System.out.println( "Batchsize: " + this.BatchSize );
    
    boolean result = true;
    
    for (int x = 0; x < this.BatchSize; x++ ) {
      
      try {
        result = this.lineParser.next(value);
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      
      if ( result ) {
      //if ( this.lineParser.hasMoreRecords()) {
        //String val_next = this.lineParser.nextRecord();
        
        long startTime = System.currentTimeMillis();

        Vector v = new RandomAccessSparseVector(this.FeatureVectorSize);
        //int actual = this.VectorFactory.processLine(value.toString(), v);
        int actual = -1;
        try {
                    
          //actual = this.VectorFactory.processLine( val_next, v );
          actual = this.VectorFactory.processLine( value.toString(), v );
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();

        batch_vec_factory_time += (endTime - startTime);
        
        // calc stats ---------
        
        double mu = Math.min(k + 1, 200);
        double ll = this.polr.logLikelihood(actual, v);  

        metrics.AvgLogLikelihood = metrics.AvgLogLikelihood + (ll - metrics.AvgLogLikelihood) / mu; 

        Vector p = new DenseVector(this.num_categories);
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
        //this.input_split.ResetToStartOfSplit();
        // nothing else to process in split!
        break;
        
      } // if
      
      
    } // for the batch size
        
    
    
    
    
    
    
    return new ParameterVectorGradientUpdatable(this.GenerateUpdate());
  }
  
  public ParameterVectorGradientUpdatable getResults() {
    return new ParameterVectorGradientUpdatable(GenerateUpdate());
  }

  /**
   * This is called when we recieve an update from the master
   * 
   * here we
   * - replace the gradient vector with the new global gradient vector
   * 
   */
  @Override
  public void update(ParameterVectorGradientUpdatable t) {
    //masterTotal = t.get();
    ParameterVectorGradient global_update = t.get();
    
    // set the local parameter vector to the global aggregate ("beta")
    this.polr.SetBeta( global_update.parameter_vector );
    
    // update global count
    this.GlobalPassCount = global_update.GlobalPassCount;
    
    // flush the local gradient delta buffer ("gamma")
    this.polr.FlushGamma();
    
  }

  @Override
  public void setup(Configuration c) {
    
    this.conf = c;
    
    try {
      

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
    }  
    
    this.SetupPOLR();
  }
  
  
  
  private void SetupPOLR() {
    
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
    
    this.polr = new ParallelOnlineLogisticRegression(this.num_categories, this.FeatureVectorSize, new UniformPrior())
    .alpha(1).stepOffset(1000)
    .decayExponent(0.9) 
    .lambda(this.Lambda)
    .learningRate(this.LearningRate);   
    
    polr_modelparams.setPOLR(polr);
    
    //this.bSetup = true;
  }  
  
  
  
  
  @Override
  public void setRecordParser(RecordParser r) {
    this.lineParser = (TextRecordParser) r;
  }


  @Override
  public ParameterVectorGradientUpdatable compute(
      List<ParameterVectorGradientUpdatable> records) {
    // TODO Auto-generated method stub
    return compute();
  }
  
  public static void main(String[] args) throws Exception {
    TextRecordParser parser = new TextRecordParser(); 
    POLRWorkerNode pwn = new POLRWorkerNode();
    ApplicationWorker<ParameterVectorGradientUpdatable> aw = new ApplicationWorker<ParameterVectorGradientUpdatable>(
        parser, pwn, ParameterVectorGradientUpdatable.class);
    
    ToolRunner.run(aw, args);
  }
}
