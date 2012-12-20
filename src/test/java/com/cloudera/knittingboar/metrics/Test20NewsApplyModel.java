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

package com.cloudera.knittingboar.metrics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.records.RecordFactory;
import com.cloudera.knittingboar.utils.DataUtils;
import com.cloudera.knittingboar.utils.DatasetConverter;
import com.cloudera.knittingboar.utils.Utils;

import junit.framework.TestCase;

public class Test20NewsApplyModel extends TestCase {

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  
  //private static Path workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")), "Test20NewsApplyModel");  
  
  
  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  
  private static File filesOutputDir = new File( workDir20NewsLocal + "/20news-converted");

  // download 20newsgroups to tmp
  private static String str20News_url = "http://people.csail.mit.edu/jrennie/20Newsgroups/20news-bydate.tar.gz";
  private static String strSaveFileAs = workDir20NewsLocal.toString() + "/20news-bydate.tar.gz";
  
  private static String str20NewsTestDirInput = "" + unzipDir.toString() + "/20news-bydate-test/";
//  private static String strTrainDirOutput = "" + filesOutputDir.toString() + "/20news-bydate-train/";
  
  private static String strKBoarTestDirInput = "" + unzipDir.toString() + "/KBoar-test/";
  //private static String str20NewsTestDirOutput = "" + filesOutputDir.toString() + "/20news-bydate-train/";
  
  
  
  // "/Users/jpatterson/Downloads/datasets/20news-kboar/models/model_10_31pm.model"
//  private static Path model20News = new Path( "/tmp/TestRunPOLRMasterAndNWorkers.20news.model" );
    private static Path model20News = new Path( "/tmp/IR_Model_0.model" );
//  private static Path model20News = new Path( "/Users/jpatterson/Downloads/datasets/20news-kboar/models/model_10_31pm.model" );
  
  //private static Path testData20News = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/test/"));  
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );

    c.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 500);
    
    // local input split path
    c.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );

    // setup 20newsgroups
    c.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", RecordFactory.TWENTYNEWSGROUPS_RECORDFACTORY);

    return c;
    
  }  
  
  public InputSplit[] generateDebugSplits( Path input_path, JobConf job ) {
    
    long block_size = localFs.getDefaultBlockSize();
    
    System.out.println("default block size: " + (block_size / 1024 / 1024) + "MB");
    
    // ---- set where we'll read the input files from -------------
    FileInputFormat.setInputPaths(job, input_path);

      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);

      int numSplits = 1;
      
      InputSplit[] splits = null;
      
      try {
        splits = format.getSplits(job, numSplits);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      return splits;
    
  }   
  
  /**
   * Check to see if a unit test has already and unzipped 20newsgroups.
   * - if not: download and unzip 20newsgroups
   * 
   * @throws MalformedURLException
   * @throws IOException
   * @throws ArchiveException
   */
  private static void CheckFor20NewsgroupsLocally() throws MalformedURLException, IOException, ArchiveException {
    
    File download_dir = new File( workDir20NewsLocal.getName() );
    
    if (!download_dir.exists()) {
      System.out.println( "Creating dir: " + workDir20NewsLocal.getName() );
      download_dir.mkdir();
    }
    
    if ( !unzipDir.exists() ) {
      System.out.println( "Creating dir: " + unzipDir.getName() );
      unzipDir.mkdir();
    }
    
    System.out.println("Looking for: " + strSaveFileAs );
    
    File existing20NewsFile = new File(strSaveFileAs);
    
    if (!existing20NewsFile.exists()) {
      //shard_file_0.delete();
      System.out.println( "Doesnt exist, downloading 20Newsgroups..." );
      Download20Newsgroups();
    } else {
      System.out.println( "Found 20Newsgroups locally. Using that Copy." );
    }
    
    System.out.println( "Unzipping 20newsgroups, bro." );
    //UnGzipAndTarFile( strSaveFileAs );

    // now convert the input test data 
    
    // ------ convert ---------
    
    System.out.println( "> Converting files in " + str20NewsTestDirInput + " to KBoar input format in: " + strKBoarTestDirInput );
    
    // make a single file
    DatasetConverter.ConvertNewsgroupsFromSingleFiles( str20NewsTestDirInput, strKBoarTestDirInput, 12000);
    
  }
  
  
  
  public static void Download20Newsgroups() throws MalformedURLException, IOException, ArchiveException {
    
    System.out.println( "Downloading: " + str20News_url );
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(str20News_url), new File(strSaveFileAs) );    
        
  }
  
  public static void UnGzipAndTarFile( String file ) throws FileNotFoundException, IOException, ArchiveException {
    
    System.out.println( "Untar-ing: " + file );
    Utils.UnTarAndZipGZFile(new File(file ), unzipDir);
    
    
  }
  

  
  public void testLoad20NewsModel() throws Exception {
    
    //CheckFor20NewsgroupsLocally();
    
    File file20News = DataUtils.getTwentyNewsGroupDir();
    

    
    DatasetConverter.ConvertNewsgroupsFromSingleFiles( "/tmp/knittingboar-20news/20news-bydate-test/", strKBoarTestDirInput, 12000);
    
    POLRModelTester tester = new POLRModelTester();
    
    // ------------------    
    // generate the debug conf ---- normally setup by YARN stuff
    tester.setConf(this.generateDebugConfigurationObject());
    // now load the conf stuff into locally used vars
    try {
      tester.LoadConfigVarsLocally();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.out.println( "Conf load fail: shutting down." );
      assertEquals( 0, 1 );
    }
    // now construct any needed machine learning data structures based on config
    tester.Setup();
    tester.Load( "/tmp/olr-news-group.model" ); //  model20News.toString() );
    
    // ------------------    
    
    Path testData20News = new Path(strKBoarTestDirInput);
 
    // ---- this all needs to be done in 
    JobConf job = new JobConf(defaultConf);

    InputSplit[] splits = generateDebugSplits(testData20News, job);
    
    System.out.println( "split count: " + splits.length );
        
      InputRecordsSplit custom_reader_0 = new InputRecordsSplit(job, splits[0]);
      tester.setupInputSplit(custom_reader_0);
      
      tester.RunThroughTestRecords();
    
  }

    
}
