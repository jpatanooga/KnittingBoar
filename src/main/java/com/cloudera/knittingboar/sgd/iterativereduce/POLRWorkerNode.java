package com.cloudera.knittingboar.sgd.iterativereduce;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.sgd.ModelDissector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.messages.GlobalParameterVectorUpdateMessage;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradient;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.metrics.POLRMetrics;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.sgd.POLRModelParameters;
import com.cloudera.knittingboar.sgd.ParallelOnlineLogisticRegression;
import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.appworker.ComputableWorker;


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
public class POLRWorkerNode  extends POLRNodeBase implements ComputableWorker<ParameterVectorGradientUpdatable, String> {
  private static final Log LOG = LogFactory.getLog(POLRWorkerNode.class);
  
  int masterTotal = 0;
  UpdateableInt workerTotal;
  
  
  
  
  
  
  

  public ParallelOnlineLogisticRegression polr = null; //lmp.createRegression();
  public POLRModelParameters polr_modelparams;
  
  public String internalID = "0";
  private RecordFactory VectorFactory = null;
  InputRecordsSplit input_split = null;
  
  
 
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
  public void ProcessIncomingParameterVectorMessage( GlobalParameterVectorUpdateMessage msg) {
    
    //this.RecvMasterParamVector(msg.parameter_vector);
    this.polr.SetBeta(msg.parameter_vector);
    
    // update global count
    this.GlobalPassCount = msg.GlobalPassCount;
    
    this.polr.FlushGamma();
  }  
  
  
  
  @Override
  public ParameterVectorGradientUpdatable compute(List<String> records) {
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
        
    
    
    
    
    
    
    return null;
  }
  
  public ParameterVectorGradientUpdatable getResults() {
    return null;
  }

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
  }
}

