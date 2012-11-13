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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.mahout.classifier.sgd.TrainLogistic;

import junit.framework.TestCase;

public class TestJobDriver extends TestCase {

  public void testBasics() throws Exception {
    
    
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    String[] params = new String[]{
        "--input", "donut.csv",
        "--output", "foo.model",
        "--features", "20",
        "--passes", "100",
        "--rate", "50"
    };
    
    ModelTrainerCmdLineDriver.mainToOutput(params, pw);
    
    String trainOut = sw.toString();
    assertTrue(trainOut.contains("Parse:correct"));
    
    
  }
  
  
  
}
