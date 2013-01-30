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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestDatasetConverter extends TestCase {

  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTrainDirInput = "" + unzipDir.toString() + "/KBoar-train4/";
  private static String strKBoarSVMLightTrainDirInput = "" + unzipDir.toString() + "/KBoar-SVMLight/";  
  
  public void test20NewsgroupsFormatConverterForNWorkers() throws Exception {
    
    //DatasetConverter.ConvertNewsgroupsFromNaiveBayesFormat("/Users/jpatterson/Downloads/datasets/20news-processed/train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train2/");
    
    //int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train3/", 5657);
    
//    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles("/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/", 2850);
    File file20News = DataUtils.getTwentyNewsGroupDir();
    
//    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarTrainDirInput, 2850);
    
    int count = DatasetConverter.ConvertNewsgroupsFromSingleFilesToSVMLightFormat( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarSVMLightTrainDirInput, 2850);
    
    assertEquals( 11314, count );
    
  }
  
}
