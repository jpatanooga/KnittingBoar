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

package com.cloudera.knittingboar.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import com.cloudera.knittingboar.records.SVMLightUtils;
import com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Tool to convert 20newsgroups to the format for Knitting Boar - need to
 * convert multiple dirs of small files into larger splits containing multiples
 * types of records per line
 * 
 * 
 * 
 * 1. Download the canonical dataset from:
 * http://people.csail.mit.edu/jrennie/20Newsgroups/20news-bydate.tar.gz 2.
 * Extract the dataset locally 3. Run the DatasetConverter process to merge the
 * smaller files into larger input files 4. edit "workDir" on line 44 in
 * com.cloudera.knittingboar.sgd.TestRunPOLRMasterAndNWorkers to reflect
 * location of input training data 5. Run unit test:
 * com.cloudera.knittingboar.sgd.TestRunPOLRMasterAndNWorkers
 * 
 * 
 * @author jpatterson
 * 
 */
public class DatasetConverter {
  
  private static void countWords(Analyzer analyzer, Collection<String> words,
      Reader in) throws IOException {
    
    // use the provided analyzer to tokenize the input stream
    TokenStream ts = analyzer.tokenStream("text", in);
    ts.addAttribute(CharTermAttribute.class);
    
    // for each word in the stream, minus non-word stuff, add word to collection
    while (ts.incrementToken()) {
      String s = ts.getAttribute(CharTermAttribute.class).toString();
      // System.out.print( " " + s );
      words.add(s);
    }
    
  }
  
  public static String ReadFullFile(Analyzer analyzer, String newsgroup_name,
      String file) throws IOException {
    
    String out = newsgroup_name + "\t";
    BufferedReader reader = null;
    // Collection<String> words
    
    Multiset<String> words = ConcurrentHashMultiset.create();
    
    try {
      reader = new BufferedReader(new FileReader(file));
      
      TokenStream ts = analyzer.tokenStream("text", reader);
      ts.addAttribute(CharTermAttribute.class);
      
      // for each word in the stream, minus non-word stuff, add word to
      // collection
      while (ts.incrementToken()) {
        String s = ts.getAttribute(CharTermAttribute.class).toString();
        out += s + " ";
      }
      
    } finally {
      if(reader != null) {
        reader.close();        
      }
    }
    
    return out + "\n";
    
  }
  
  /**
   * Function to convert the 20Newsgroups from the standard 20,000 files in 20
   * directories to N files more appropriate for Knitting Boar
   * 
   * 1. Download the 20Newsgroups dataset from:
   * http://people.csail.mit.edu/jrennie/20Newsgroups/20news-bydate.tar.gz 2.
   * Extract the dataset to a local dir 3. Run the DatasetConverter process to
   * merge the smaller files into larger input files check out
   * "TestConvert20NewsTestDataset" in the unit tests
   * 
   * 
   * @param inputBaseDir
   * @param outputBaseDir
   * @throws IOException
   */
  public static int ConvertNewsgroupsFromSingleFiles(String inputBaseDir,
      String outputBaseDir, int records_per_shard) throws IOException {
    
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
    
    File base = new File(inputBaseDir);
    
    // because OLR expects to get integer class IDs for the target variable
    // during training
    // we need a dictionary to convert the target variable (the newsgroup name)
    // to an integer, which is the newsGroup object
    List<File> files = new ArrayList<File>();
    for (File newsgroup : base.listFiles()) {
      files.addAll(Arrays.asList(newsgroup.listFiles()));
    }
    
    // mix up the files, helps training in OLR
    Collections.shuffle(files);
    System.out.printf("%d training files\n", files.size());
    
    double step = 0.0;
    int[] bumps = new int[] {1, 2, 5};
    
    BufferedWriter shard_writer = null;
    int shard_count = 0;
    int input_file_count = 0;
    Map<Integer,Integer> current_shard_rec_count = new HashMap<Integer,Integer>();
    
    try {
      
      File base_dir = new File(outputBaseDir);
      if (!base_dir.exists()) {
        base_dir.mkdirs();
      }
      
      File shard_file_0 = new File(outputBaseDir + "kboar-shard-" + shard_count
          + ".txt");
      
      if (shard_file_0.exists()) {
        shard_file_0.delete();
      }
      
      shard_file_0.createNewFile();
      shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
      
      System.out.println("Starting: " + shard_file_0.toString());
      
      // ----- "reading and tokenzing the data" ---------
      for (File file : files) {
        
        input_file_count++;
        
        // identify newsgroup ----------------
        // convert newsgroup name to unique id
        // -----------------------------------
        String ng = file.getParentFile().getName();
        
        String file_contents = ReadFullFile(analyzer, file.getParentFile()
            .getName(), inputBaseDir + file.getParentFile().getName() + "/"
            + file.getName());
        
        shard_writer.write(file_contents);
        
        if (false == current_shard_rec_count.containsKey(shard_count)) {
          
          System.out.println(".");
          current_shard_rec_count.put(shard_count, 1);
          
        } else {
          int c = current_shard_rec_count.get(shard_count);
          
          current_shard_rec_count.put(shard_count, ++c);
        }
        
        if (current_shard_rec_count.get(shard_count) >= records_per_shard) {
          
          shard_writer.flush();
          shard_writer.close();
          
          shard_count++;
          
          shard_file_0 = new File(outputBaseDir + "kboar-shard-" + shard_count
              + ".txt");
          
          System.out.println("Starting shard: " + "kboar-shard-" + shard_count
              + ".txt");
          
          if (shard_file_0.exists()) {
            shard_file_0.delete();
          }
          
          shard_file_0.createNewFile();
          
          shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
          
        }
        
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
        
        if (input_file_count % (bump * scale) == 0) {
          step += 0.25;
          System.out.printf("Files Converted: %10d , %10d \n",
              input_file_count, current_shard_rec_count.get(shard_count));
        }
        
      } // for
      
    } finally {
      if(shard_writer != null) {
        shard_writer.flush();
        shard_writer.close();        
      }
    }
    
    for (int x = 0; x < current_shard_rec_count.size(); x++) {
      
      System.out.println("> Shard " + x + " record count: "
          + current_shard_rec_count.get(x));
      
    }
    
    System.out.printf("> Total Files Converted: %10d \n", input_file_count);
    
    return input_file_count;
    
  }
  
  
  
  
  
  /**
   * Function to convert the 20Newsgroups from the standard 20,000 files in 20
   * directories to N files in the SVMLight format
   * 
   * 
   * @param inputBaseDir
   * @param outputBaseDir
   * @throws Exception 
   */
  public static int ConvertNewsgroupsFromSingleFilesToSVMLightFormat(String inputBaseDir,
      String outputBaseDir, int records_per_shard) throws Exception {
    
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
    
    File base = new File(inputBaseDir);
    
    // because OLR expects to get integer class IDs for the target variable
    // during training
    // we need a dictionary to convert the target variable (the newsgroup name)
    // to an integer, which is the newsGroup object
    List<File> files = new ArrayList<File>();
    for (File newsgroup : base.listFiles()) {
      files.addAll(Arrays.asList(newsgroup.listFiles()));
    }
    
    // mix up the files, helps training in OLR
    Collections.shuffle(files);
    System.out.printf("%d training files\n", files.size());
    
    double step = 0.0;
    int[] bumps = new int[] {1, 2, 5};
    
    BufferedWriter shard_writer = null;
    int shard_count = 0;
    int input_file_count = 0;
    Map<Integer,Integer> current_shard_rec_count = new HashMap<Integer,Integer>();
    
    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory("\t");
    
    
    try {
      
      File base_dir = new File(outputBaseDir);
      if (!base_dir.exists()) {
        base_dir.mkdirs();
      }
      
      File shard_file_0 = new File(outputBaseDir + "kboar-shard-" + shard_count
          + ".txt");
      
      if (shard_file_0.exists()) {
        shard_file_0.delete();
      }
      
      shard_file_0.createNewFile();
      shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
      
      System.out.println("Starting: " + shard_file_0.toString());
      
      // ----- "reading and tokenzing the data" ---------
      for (File file : files) {
        
        input_file_count++;
        
        // identify newsgroup ----------------
        // convert newsgroup name to unique id
        // -----------------------------------
        String ng = file.getParentFile().getName();
        
        String file_contents = ReadFullFile(analyzer, file.getParentFile()
            .getName(), inputBaseDir + file.getParentFile().getName() + "/"
            + file.getName());
        
        //System.out.println( "> " + file_contents );
        
        Vector v_0 = new RandomAccessSparseVector( TwentyNewsgroupsRecordFactory.FEATURES );
        
        int actual_0 = rec_factory.processLine(file_contents, v_0);
        
        
        //shard_writer.write(file_contents);
        String res = SVMLightUtils.ConvertVectorIntoSVMLightFormat(v_0, actual_0 + "");
        
        shard_writer.write( res );
        
        if (false == current_shard_rec_count.containsKey(shard_count)) {
          
          System.out.println(".");
          current_shard_rec_count.put(shard_count, 1);
          
        } else {
          int c = current_shard_rec_count.get(shard_count);
          
          current_shard_rec_count.put(shard_count, ++c);
        }
        
        if (current_shard_rec_count.get(shard_count) >= records_per_shard) {
          
          shard_writer.flush();
          shard_writer.close();
          
          shard_count++;
          
          shard_file_0 = new File(outputBaseDir + "kboar-shard-" + shard_count
              + ".txt");
          
          System.out.println("Starting shard: " + "kboar-shard-" + shard_count
              + ".txt");
          
          if (shard_file_0.exists()) {
            shard_file_0.delete();
          }
          
          shard_file_0.createNewFile();
          
          shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
          
        }
        
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
        
        if (input_file_count % (bump * scale) == 0) {
          step += 0.25;
          System.out.printf("Files Converted: %10d , %10d \n",
              input_file_count, current_shard_rec_count.get(shard_count));
        }
        
      } // for
      
    } finally {
      if(shard_writer != null) {
        shard_writer.flush();
        shard_writer.close();        
      }
    }
    
    for (int x = 0; x < current_shard_rec_count.size(); x++) {
      
      System.out.println("> Shard " + x + " record count: "
          + current_shard_rec_count.get(x));
      
    }
    
    System.out.printf("> Total Files Converted: %10d \n", input_file_count);
    
    return input_file_count;
    
  }  
  
  
  
  
  
  
  /**
   * Conversion Tool to break up the RCV1 dataset into smaller chunks for
   * various tests.
   * 
   * RCV1 Dataset:
   * 
   * https://github.com/JohnLangford/vowpal_wabbit/wiki/Rcv1-example
   * 
   * @param input_file
   * @param outputBaseDir
   * @param total_recs_to_extract
   * @param records_per_shard
   * @return
   * @throws IOException
   */
  public static int ExtractSubsetofRCV1V2ForTraining(String input_file,
      String outputBaseDir, int total_recs_to_extract, int records_per_shard)
      throws IOException {
    
    double step = 0.0;
    int[] bumps = new int[] {1, 2, 5};
    
    System.out.println("> ExtractSubsetofRCV1V2ForTraining: " + input_file);
    
    BufferedWriter shard_writer = null;
    int shard_count = 0;
    int input_file_count = 0;
    int line_count = 0;
    Map<Integer,Integer> current_shard_rec_count = new HashMap<Integer,Integer>();
    
    try {
      
      File base_dir = new File(outputBaseDir);
      if (!base_dir.exists()) {
        base_dir.mkdirs();
      }
      
      System.out.println(outputBaseDir + "rcv1-shard-" + shard_count + ".txt");
      
      File shard_file_0 = new File(outputBaseDir + "rcv1-shard-" + shard_count
          + ".txt");
      
      if (shard_file_0.exists()) {
        shard_file_0.delete();
      } else {
        
        System.out.println("no output file, creating...");
        
      }
      
      boolean bCreate = shard_file_0.createNewFile();
      
      System.out.println("file created: " + bCreate);
      
      shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
      
      input_file_count++;
      
      BufferedReader reader = null;
      
      try {
        System.out.println("opening file for reading: input_file");
        reader = new BufferedReader(new FileReader(input_file));
        
        String line = reader.readLine();
        
        while (line != null && line.length() > 0) {
          
          shard_writer.write(line + "\n");
          
          line = reader.readLine();
          
          if (false == current_shard_rec_count.containsKey(shard_count)) {
            
            current_shard_rec_count.put(shard_count, 1);
            
          } else {
            int c = current_shard_rec_count.get(shard_count);
            
            current_shard_rec_count.put(shard_count, ++c);
          }
          
          line_count++;
          
          if (total_recs_to_extract <= line_count) {
            break;
          }
          
          if (current_shard_rec_count.get(shard_count) >= records_per_shard) {
            
            shard_writer.flush();
            shard_writer.close();
            shard_count++;
            
            shard_file_0 = new File(outputBaseDir + "rcv1-shard-" + shard_count
                + ".txt");
            
            System.out.println("Starting shard: " + "rcv1-shard-" + shard_count
                + ".txt");
            
            if (shard_file_0.exists()) {
              shard_file_0.delete();
            }
            
            shard_file_0.createNewFile();
            
            shard_writer = new BufferedWriter(new FileWriter(shard_file_0));
            
          }
          
          int bump = bumps[(int) Math.floor(step) % bumps.length];
          int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
          
          if (input_file_count % (bump * scale) == 0) {
            step += 0.25;
            System.out.printf("Files Converted: %10d , %10d \n",
                input_file_count, current_shard_rec_count.get(shard_count));
          }
          
        }
        
      } finally {
        reader.close();
      }
      
    } catch (Exception e) {
      
      System.out.println(e);
      
    } finally {
      shard_writer.flush();
      shard_writer.close();
    }
    
    for (int x = 0; x < current_shard_rec_count.size(); x++) {
      
      System.out.println("> Shard " + x + " record count: "
          + current_shard_rec_count.get(x));
      
    }
    
    System.out.printf("> Total Files Converted: %10d \n", input_file_count);
    
    return input_file_count;
  }
  
}
