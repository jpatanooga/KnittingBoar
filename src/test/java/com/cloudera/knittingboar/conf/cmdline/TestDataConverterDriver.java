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

package com.cloudera.knittingboar.conf.cmdline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.cloudera.knittingboar.utils.Utils;

import junit.framework.TestCase;

public class TestDataConverterDriver extends TestCase {

  
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
  
  private static Path workDir = new Path(new Path(System.getProperty("test.build.data", "/tmp")), "TestDataConverterDriver");  
  private static File unzipDir = new File( workDir + "/20news-bydate");
  
  private static File filesOutputDir = new File( workDir + "/20news-converted");

  // download 20newsgroups to tmp
  private static String str20News_url = "http://people.csail.mit.edu/jrennie/20Newsgroups/20news-bydate.tar.gz";
  private static String strSaveFileAs = workDir.toString() + "/20news-bydate.tar.gz";
  
  // "/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/"
  private static String strTrainDirInput = "" + unzipDir.toString() + "/20news-bydate-train/";
  private static String strTrainDirOutput = "" + filesOutputDir.toString() + "/20news-bydate-train/";
  
  public static void Download20Newsgroups() throws MalformedURLException, IOException, ArchiveException {
    
    System.out.println( "Downloading: " + str20News_url );
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(str20News_url), new File(strSaveFileAs) );    
        
  }
  
  public static void UnTar20News() throws FileNotFoundException, IOException, ArchiveException {
    
    System.out.println( "Untar-ing: " + strSaveFileAs );
    Utils.UnTarAndZipGZFile(new File(workDir.toString() + "/20news-bydate.tar.gz" ), unzipDir);
    
    
  }
  
  /**
   * Becomes I'm lazy and the java version had bugs and stuff.
   * @throws IOException 
   */
  public static void UnTarExecVersion() throws IOException {
    
    System.out.println( "UnTarExecVersion: Untar-ing: " + strSaveFileAs );
    
    System.out.println( "exec: " + "tar -C " + unzipDir + " -xvf " + strSaveFileAs );
    
//    return;
    
    Process p = Runtime.getRuntime().exec( "tar -C " + unzipDir + " -xvf " + strSaveFileAs);
    //"ls -lah /tmp/" ); //
    
//    BufferedReader stdInput = new BufferedReader(new 
//         InputStreamReader(p.getInputStream()));

    BufferedReader stdError = new BufferedReader(new 
         InputStreamReader(p.getErrorStream()));

    String s = "";
    // read the output from the command
//    System.out.println("Here is the standard output of the command:\n");
//    while ((s = stdInput.readLine()) != null) {
//        System.out.println(s);
//    }
    
    // read any errors from the attempted command
    System.out.println("Here is the standard error of the command (if any):\n");
    while ((s = stdError.readLine()) != null) {
        System.out.println(s);
    }    
    
    
  }
  
  public void testBasics() throws Exception {
    
    File download_dir = new File("/tmp/TestDataConverterDriver");
    
    if (!download_dir.exists()) {
      System.out.println( "Creating dir..." );
      download_dir.mkdir();
    }
    
    if ( !unzipDir.exists() ) {
      unzipDir.mkdir();
    }
    
    System.out.println("Looking for: " + strSaveFileAs );
    
    File existing20NewsFile = new File(strSaveFileAs);
    
    if (!existing20NewsFile.exists()) {
      //shard_file_0.delete();
      System.out.println( "Doesnt exist, downloading 20Newsgroups..." );
      Download20Newsgroups();
    }
    
    System.out.println( "Unzipping 20newsgroups, bro." );
    UnTarExecVersion();
    
    
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    String[] params = new String[]{
        "--input", strTrainDirInput,
        "--output", strTrainDirOutput,
        "--recordsPerBlock", "2000"
    };
    
    DataConverterCmdLineDriver.mainToOutput(params, pw);
    
    String trainOut = sw.toString();
    assertTrue(trainOut.contains("Total Records Converted: 11314"));
    
    //System.out.println( workDir.toString() + "/20news-bydate.tar.gz" );
   
    File dir = new File(strTrainDirOutput);
    File[] files = dir.listFiles();
    for (int x = 0; x < files.length; x++ ) {
      System.out.println(files[x].toString());
    }
    
    assertEquals( 6, files.length );
    
    
    
  }
  
  
  
}
