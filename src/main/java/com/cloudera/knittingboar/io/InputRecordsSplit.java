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

package com.cloudera.knittingboar.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Encapsulates functionality from:
 * 
 * - FileInputFormat::getSplits(...)
 *    - this info should be calculated by the main job controlling process [MOVE]
 * 
 * - TextInputFormat::readSplit(...)
 *  
 * 
 * Notes
 * -  currently hard-coded to read CSV "record per line" non-compressed records from disk
 * 
 * @author jpatterson
 *
 */
public class InputRecordsSplit {
  
  TextInputFormat input_format = null;
  InputSplit split = null;
  JobConf jobConf = null;
  
  RecordReader<LongWritable, Text> reader = null;
  LongWritable key = null;
  
  final Reporter voidReporter = Reporter.NULL;
  
  public InputRecordsSplit(JobConf jobConf, InputSplit split) throws IOException {
    
    this.jobConf = jobConf;
    this.split = split;
    this.input_format = new TextInputFormat();

    // RecordReader<LongWritable, Text> reader = format.getRecordReader(splits[x], job, reporter);
    this.reader = input_format.getRecordReader(this.split, this.jobConf, voidReporter);
    this.key = reader.createKey();
    //Text value = reader.createValue();
    
    
  }
  
  /**
   * 
   * just a dead simple way to do this
   *
   * - functionality from TestTextInputFormat::readSplit()
   * 
   * If returns true, then csv_line contains the next line
   * If returns false, then there is no next record
   * 
   * Will terminate when it hits the end of the split based on the information provided in the split class
   * to the constructor and the TextInputFormat
   * 
   * @param csv_line
   * @throws IOException 
   */
  public boolean next(Text csv_line) throws IOException {
    
    return reader.next(key, csv_line);
    
  }
  
  public void ResetToStartOfSplit() throws IOException {
    
    // I'mma cheatin here. sue me.
    this.reader = input_format.getRecordReader(this.split, this.jobConf, voidReporter);
    
  }

}
