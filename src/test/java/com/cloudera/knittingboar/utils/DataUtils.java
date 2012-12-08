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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.io.FileUtils;

public class DataUtils {
  
  private static File twentyNewsGroups;
  private static final String TWENTY_NEWS_GROUP_LOCAL_DIR = "knittingboar-20news";
  private static final String TWENTY_NEWS_GROUP_TAR_URL = "http://people.csail.mit.edu/jrennie/20Newsgroups/20news-bydate.tar.gz";
  private static final String TWENTY_NEWS_GROUP_TAR_FILE_NAME = "20news-bydate.tar.gz";
  
  public static synchronized File getTwentyNewsGroupDir() throws IOException {
    if(twentyNewsGroups != null) {
      return twentyNewsGroups;
    }
    // mac gives unique tmp each run and we want to store this persist
    // this data across restarts
    File tmpDir = new File("/tmp");
    if(!tmpDir.isDirectory()) {
      tmpDir = new File(System.getProperty("java.io.tmpdir"));
    }
    File baseDir = new File(tmpDir, TWENTY_NEWS_GROUP_LOCAL_DIR);
    if(!(baseDir.isDirectory() || baseDir.mkdir())) {
      throw new IOException("Could not mkdir " + baseDir);
    }
    File tarFile = new File(baseDir, TWENTY_NEWS_GROUP_TAR_FILE_NAME);
    
    if(!tarFile.isFile()) {
      FileUtils.copyURLToFile(new URL(TWENTY_NEWS_GROUP_TAR_URL), tarFile);      
    }
    
    Process p = Runtime.getRuntime().exec(String.format("tar -C %s -xvf %s", 
        baseDir.getAbsolutePath(), tarFile.getAbsolutePath()));
    BufferedReader stdError = new BufferedReader(new 
        InputStreamReader(p.getErrorStream()));
    System.out.println("Here is the standard error of the command (if any):\n");
    String s;
    while ((s = stdError.readLine()) != null) {
        System.out.println(s);
    }
    stdError.close();
    twentyNewsGroups = baseDir;
    return twentyNewsGroups;
  }
}
