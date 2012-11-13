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

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

/**
 * Adapted from:
 * 
 * https://github.com/tdunning/MiA/blob/master/src/main/java/mia/classifier/ch14/TrainNewsGroups.java
 * 
 * 
 * I've hardcoded the class id's in the dataset record factory, cause, uh, they don't really change
 * in this dataset
 * 
 * @author jpatterson
 *
 */
public class TwentyNewsgroupsRecordFactory implements RecordFactory { // implements RecordFactory {

  public static final int FEATURES = 10000;
  
  Dictionary newsGroups = null; //new Dictionary();
  
  Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
  
  String class_id_split_string = " ";
  
  public TwentyNewsgroupsRecordFactory( String strClassSeperator ) {
    
    this.newsGroups = new Dictionary();
    
    newsGroups.intern( "alt.atheism" );
    newsGroups.intern( "comp.graphics" );
    newsGroups.intern( "comp.os.ms-windows.misc" );
    newsGroups.intern( "comp.sys.ibm.pc.hardware" );
    newsGroups.intern( "comp.sys.mac.hardware" );
    newsGroups.intern( "comp.windows.x" );
    newsGroups.intern( "misc.forsale" );
    newsGroups.intern( "rec.autos" );
    newsGroups.intern( "rec.motorcycles" );
    newsGroups.intern( "rec.sport.baseball" );
    
    newsGroups.intern( "rec.sport.hockey" );
    newsGroups.intern( "sci.crypt" );
    newsGroups.intern( "sci.electronics" );
    newsGroups.intern( "sci.med" );
    newsGroups.intern( "sci.space" );
    newsGroups.intern( "soc.religion.christian" );
    newsGroups.intern( "talk.politics.guns" );
    newsGroups.intern( "talk.politics.mideast" );
    newsGroups.intern( "talk.politics.misc" );
    newsGroups.intern( "talk.religion.misc" );
    
    this.class_id_split_string = strClassSeperator;
    
  }
  
  @Override
  public List<String> getTargetCategories() {
    
    List<String> out = new ArrayList<String>();
    
    for ( int x = 0; x < this.newsGroups.size(); x++ ) {
      
      //System.out.println( x + "" + this.newsGroups.values().get(x) );
      out.add(this.newsGroups.values().get(x));
      
    }
    
    return out;
    
  }
  
  public int LookupIDForNewsgroupName(String name) {
    
    return this.newsGroups.values().indexOf(name);
    
  }
 
  public boolean ContainsIDForNewsgroupName(String name) {
    
    return this.newsGroups.values().contains(name);
    
  }
  
  public String GetNewsgroupNameByID( int id ) {
    
    return this.newsGroups.values().get(id);
    
  }
  
  @Override
  public String GetClassnameByID(int id) {
    return this.newsGroups.values().get(id);
  }
  
  private static void countWords(Analyzer analyzer, Collection<String> words, Reader in) throws IOException {
    
    // use the provided analyzer to tokenize the input stream
    TokenStream ts = analyzer.tokenStream("text", in);
    ts.addAttribute(CharTermAttribute.class);
    
    // for each word in the stream, minus non-word stuff, add word to collection
    while (ts.incrementToken()) {
      String s = ts.getAttribute(CharTermAttribute.class).toString();
      words.add(s);
    }
    
  }  
  
  
  
  /**
   * Processes single line of input into:
   * - target variable
   * - Feature vector
   * @throws Exception 
   */
  public int processLine(String line, Vector v) throws Exception {

    
    String[] parts = line.split(this.class_id_split_string);
    if (parts.length < 2) {
      throw new Exception( "wtf: line not formed well." );
    }
    
    String newsgroup_name = parts[0];
    String msg = parts[1];
    
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
     
        int actual = newsGroups.intern(newsgroup_name);
//        newsGroups.values().contains(arg0)
        
       // System.out.println( "> newsgroup name: " + newsgroup_name );
       // System.out.println( "> newsgroup id: " + actual );
        
        Multiset<String> words = ConcurrentHashMultiset.create();
/*        
   //     System.out.println("record: ");
        for ( int x = 1; x < parts.length; x++ ) {
          //String s = ts.getAttribute(CharTermAttribute.class).toString();
     //     System.out.print( " " + parts[x] );
          String foo = parts[x].trim();
          System.out.print( " " + foo );
          words.add( foo );
          
        }        
    //    System.out.println("\nEOR");
        System.out.println( "\nwords found: " + (parts.length - 1) );
        System.out.println( "words in set: " + words.size() + ", " + words.toString() );
*/
        


        StringReader in = new StringReader(msg);
        
        countWords(analyzer, words, in);    

        
        // ----- p.271 -----------
        //Vector v = new RandomAccessSparseVector(FEATURES);
        
        // original value does nothing in a ContantValueEncoder
        bias.addToVector("", 1, v);
        
        // original value does nothing in a ContantValueEncoder
        //lines.addToVector("", lineCount / 30, v);

        // original value does nothing in a ContantValueEncoder        
        //logLines.addToVector("", Math.log(lineCount + 1), v);
  
        // now scan through all the words and add them
        //System.out.println( "############### " + words.toArray().length);
        for (String word : words.elementSet()) {
          encoder.addToVector(word, Math.log(1 + words.count(word)), v);
          //System.out.print( words.count(word) + " " );
        }        

//        System.out.println("\nEOL\n");
        
        return actual;
  }
  
  public void Debug() {
    
    System.out.println( "DictionarySize: " + this.newsGroups.values().size() );
    
  }
  
  public void DebugDictionary() {
    
    for ( int x = 0; x < this.newsGroups.size(); x++ ) {
      
      System.out.println( x + "" + this.newsGroups.values().get(x) );
      
    }
    
  }



}
