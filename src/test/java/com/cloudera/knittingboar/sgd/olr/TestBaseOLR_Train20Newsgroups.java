package com.cloudera.knittingboar.sgd.olr;

import junit.framework.TestCase;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;

import com.cloudera.knittingboar.utils.Utils;
import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.io.Closeables;

/**
 * Run basic OLR on the 20 newsgroups dataset
 * - collect stats on run
 * -- accuracy
 * -- wall clock time
 * 
 * Credit: adapted from:
 * 
 * https://github.com/tdunning/MiA/blob/master/src/main/java/mia/classifier/ch14/TrainNewsGroups.java
 * 
 * @author jpatterson
 *
 */
public class TestBaseOLR_Train20Newsgroups extends TestCase {
  
  private static final int FEATURES = 10000;
  private static Multiset<String> overallCounts;

  
  /**
   * 
   * Counts words
   * 
   * @param analyzer
   * @param words
   * @param in
   * @throws IOException
   */
  private static void countWords(Analyzer analyzer, Collection<String> words, Reader in) throws IOException {
    
    //System.out.println( "> ----- countWords ------" );
    
    // use the provided analyzer to tokenize the input stream
    TokenStream ts = analyzer.tokenStream("text", in);
    ts.addAttribute(CharTermAttribute.class);
    
    // for each word in the stream, minus non-word stuff, add word to collection
    while (ts.incrementToken()) {
      String s = ts.getAttribute(CharTermAttribute.class).toString();
      //System.out.print( " " + s );
      words.add(s);
    }
    
    //System.out.println( "\n<" );
    
    /*overallCounts.addAll(words);*/
  }  
  
  

  
  
  public void testTrainNewsGroups() throws IOException {
    

    File base = new File("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/");
    overallCounts = HashMultiset.create();

    long startTime = System.currentTimeMillis();


    
    // p.269 ---------------------------------------------------------
    Map<String, Set<Integer>> traceDictionary = new TreeMap<String, Set<Integer>>();
    
    // encodes the text content in both the subject and the body of the email
    FeatureVectorEncoder encoder = new StaticWordValueEncoder("body");
    encoder.setProbes(2);
    encoder.setTraceDictionary(traceDictionary);
    
    // provides a constant offset that the model can use to encode the average frequency 
    // of each class
    FeatureVectorEncoder bias = new ConstantValueEncoder("Intercept");
    bias.setTraceDictionary(traceDictionary);
    
    // used to encode the number of lines in a message
    FeatureVectorEncoder lines = new ConstantValueEncoder("Lines");
    lines.setTraceDictionary(traceDictionary);
    
    FeatureVectorEncoder logLines = new ConstantValueEncoder("LogLines");
    logLines.setTraceDictionary(traceDictionary);
    
    
    Dictionary newsGroups = new Dictionary();

    
    // matches the OLR setup on p.269 ---------------
    // stepOffset, decay, and alpha --- describe how the learning rate decreases
    // lambda: amount of regularization
    // learningRate: amount of initial learning rate
    OnlineLogisticRegression learningAlgorithm = 
        new OnlineLogisticRegression(
              20, FEATURES, new L1())
            .alpha(1).stepOffset(1000)
            .decayExponent(0.9) 
            .lambda(3.0e-5)
            .learningRate(20);

    // bottom of p.269 ------------------------------
    // because OLR expects to get integer class IDs for the target variable during training
    // we need a dictionary to convert the target variable (the newsgroup name)
    // to an integer, which is the newsGroup object
    List<File> files = new ArrayList<File>();
    for (File newsgroup : base.listFiles()) {
      newsGroups.intern(newsgroup.getName());
      files.addAll(Arrays.asList(newsgroup.listFiles()));
    }

    // mix up the files, helps training in OLR
    Collections.shuffle(files);
    System.out.printf("%d training files\n", files.size());

    
    
    // p.270 ----- metrics to track lucene's parsing mechanics, progress, performance of OLR ------------
    double averageLL = 0.0;
    double averageCorrect = 0.0;
    double averageLineCount = 0.0;
    int k = 0;
    double step = 0.0;
    int[] bumps = new int[]{1, 2, 5};
    double lineCount = 0;

    // last line on p.269
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);

    Splitter onColon = Splitter.on(":").trimResults();
    
    
    int input_file_count = 0;
    
    // ----- p.270 ------------ "reading and tokenzing the data" ---------
    for (File file : files) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        
        input_file_count++;
        
        // identify newsgroup ----------------
        // convert newsgroup name to unique id
        // -----------------------------------
        String ng = file.getParentFile().getName();     
        int actual = newsGroups.intern(ng);
        Multiset<String> words = ConcurrentHashMultiset.create();
        
        // check for line count header -------
        String line = reader.readLine();
        while (line != null && line.length() > 0) {
          
          // if this is a line that has a line count, let's pull that value out ------
          if (line.startsWith("Lines:")) {              
            String count = Iterables.get(onColon.split(line), 1);
            try {
              lineCount = Integer.parseInt(count);
              averageLineCount += (lineCount - averageLineCount) / Math.min(k + 1, 1000);
            } catch (NumberFormatException e) {
              // if anything goes wrong in parse: just use the avg count
              lineCount = averageLineCount;
            }
          }
        
          
          boolean countHeader = (
              line.startsWith("From:") || line.startsWith("Subject:")||
              line.startsWith("Keywords:")|| line.startsWith("Summary:"));
        
          
          
          // loop through the lines in the file, while the line starts with: " "
          do {

            // get a reader for this specific string ------
            StringReader in = new StringReader(line);

            // ---- count words in header ---------            
            if (countHeader) {
              countWords(analyzer, words, in);    
            }
            
            // iterate to the next string ----
            line = reader.readLine();
            
          } while (line.startsWith(" "));
        
        } // while (lines in header) {
        
        //  -------- count words in body ----------
        countWords(analyzer, words, reader);      
        reader.close();
        
        
        // ----- p.271 -----------
        Vector v = new RandomAccessSparseVector(FEATURES);
        
        // original value does nothing in a ContantValueEncoder
        bias.addToVector("", 1, v);
        
        // original value does nothing in a ContantValueEncoder
        lines.addToVector("", lineCount / 30, v);

        // original value does nothing in a ContantValueEncoder        
        logLines.addToVector("", Math.log(lineCount + 1), v);
  
        // now scan through all the words and add them
        for (String word : words.elementSet()) {
          encoder.addToVector(word, Math.log(1 + words.count(word)), v);
        }
        
        //Utils.PrintVectorNonZero(v);

        // calc stats ---------
        
        double mu = Math.min(k + 1, 200);
        double ll = learningAlgorithm.logLikelihood(actual, v);  
        averageLL = averageLL + (ll - averageLL) / mu;

        Vector p = new DenseVector(20);
        learningAlgorithm.classifyFull(p, v);
        int estimated = p.maxValueIndex();

        int correct = (estimated == actual? 1 : 0);
        averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
        
        learningAlgorithm.train(actual, v);
        
        
        k++;
        
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
        
        if (k % (bump * scale) == 0) {
          step += 0.25;
          System.out.printf("%10d %10.3f %10.3f %10.2f %s %s\n",
               k, ll, averageLL, averageCorrect * 100, ng, 
               newsGroups.values().get(estimated));
        }
        
        learningAlgorithm.close();  
                
        
    /*    if (k>4) {
          break;
        }
      */
        
        
        
    }
    

    long endTime = System.currentTimeMillis();

    //System.out.println("That took " + (endTime - startTime) + " milliseconds");
    long duration = (endTime - startTime);

    
    System.out.println( "Processed Input Files: " + input_file_count +", time: " + duration + "ms" );
    
    ModelSerializer.writeBinary("/tmp/olr-news-group.model", learningAlgorithm);
       // learningAlgorithm.getBest().getPayload().getLearner().getModels().get(0));

    
    






    
    
    
    
  }

  

}
