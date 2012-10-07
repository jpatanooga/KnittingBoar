package com.cloudera.knittingboar.records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

import junit.framework.TestCase;

public class Test20NewsgroupsBookParsing extends TestCase {
  
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
    
    System.out.println( "> ----- countWords ------" );
    
    // use the provided analyzer to tokenize the input stream
    TokenStream ts = analyzer.tokenStream("text", in);
    ts.addAttribute(CharTermAttribute.class);
    
    // for each word in the stream, minus non-word stuff, add word to collection
    while (ts.incrementToken()) {
      String s = ts.getAttribute(CharTermAttribute.class).toString();
      System.out.print( " " + s );
      words.add(s);
    }
    
    System.out.println( "\n<" );
    
    /*overallCounts.addAll(words);*/
  }  
  
  
  public void test20NewsgroupsFileScan() throws IOException {
    
    // p.270 ----- metrics to track lucene's parsing mechanics, progress, performance of OLR ------------
    double averageLL = 0.0;
    double averageCorrect = 0.0;
    double averageLineCount = 0.0;
    int k = 0;
    double step = 0.0;
    int[] bumps = new int[]{1, 2, 5};
    double lineCount = 0;

    
    Splitter onColon = Splitter.on(":").trimResults();
    // last line on p.269
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
    

    File base = new File("/Users/jpatterson/Downloads/datasets/20news-bydate/20-debug/");
    overallCounts = HashMultiset.create();

    
    
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
    Dictionary newsGroups = new Dictionary();
    

    // bottom of p.269 ------------------------------
    // because OLR expects to get integer class IDs for the target variable during training
    // we need a dictionary to convert the target variable (the newsgroup name)
    // to an integer, which is the newsGroup object
    List<File> files = new ArrayList<File>();
    for (File newsgroup : base.listFiles()) {
      newsGroups.intern(newsgroup.getName());
      System.out.println( ">> " + newsgroup.getName() );
      files.addAll(Arrays.asList(newsgroup.listFiles()));
    }

    // mix up the files, helps training in OLR
    Collections.shuffle(files);
    System.out.printf("%d training files\n", files.size());
    
    
    
    // ----- p.270 ------------ "reading and tokenzing the data" ---------
    for (File file : files) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        
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
        
          
          // which header words to actually count
          boolean countHeader = (
              line.startsWith("From:") || line.startsWith("Subject:")||
              line.startsWith("Keywords:")|| line.startsWith("Summary:"));
        
          
          // we're still looking at the header at this point
          // loop through the lines in the file, while the line starts with: " "
          do {

            // get a reader for this specific string ------
            StringReader in = new StringReader(line);

            
            
            // ---- count words in header ---------            
            if (countHeader) {
              //System.out.println( "#### countHeader ################*************" );
              countWords(analyzer, words, in);    
            }
            
            // iterate to the next string ----
            line = reader.readLine();
            
            
            
          } while (line.startsWith(" "));
        
          //System.out.println("[break]");
        
        }
        
        // now we're done with the header
        
        //System.out.println("[break-header]");
        
        //  -------- count words in body ----------
        countWords(analyzer, words, reader);      
        reader.close();
        
/*        
        for (String word : words.elementSet()) {
          //encoder.addToVector(word, Math.log(1 + words.count(word)), v);
         System.out.println( "> " + word + ", " + words.count(word) );
        }        
*/        
      }
    
    
  }  
  
}
