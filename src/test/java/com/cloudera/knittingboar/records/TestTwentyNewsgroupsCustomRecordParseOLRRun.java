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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.io.InputRecordsSplit;
import com.cloudera.knittingboar.utils.TestingUtils;
import com.google.common.io.Files;

public class TestTwentyNewsgroupsCustomRecordParseOLRRun {
  private static final Log LOG = LogFactory
      .getLog(TestTwentyNewsgroupsCustomRecordParseOLRRun.class.getName());

  private static final int FEATURES = 10000;

  private JobConf defaultConf;
  private FileSystem localFs;

  private File baseDir;
  private Path workDir;
  private String inputFileName;

  @Before
  public void setup() throws Exception {
    defaultConf = new JobConf();
    defaultConf.set("fs.defaultFS", "file:///");
    localFs = FileSystem.getLocal(defaultConf);
    inputFileName = "kboar-shard-0.txt";
    baseDir = Files.createTempDir();
    File inputFile = new File(baseDir, inputFileName);
    TestingUtils.copyDecompressed(inputFileName + ".gz", inputFile);
    workDir = new Path(baseDir.getAbsolutePath());
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(baseDir);
  }

  @Test
  public void testRecordFactoryOnDatasetShard() throws Exception {
    // TODO a test with assertions is not a test
    // p.270 ----- metrics to track lucene's parsing mechanics, progress,
    // performance of OLR ------------
    double averageLL = 0.0;
    double averageCorrect = 0.0;
    int k = 0;
    double step = 0.0;
    int[] bumps = new int[] { 1, 2, 5 };

    TwentyNewsgroupsRecordFactory rec_factory = new TwentyNewsgroupsRecordFactory(
        "\t");
    // rec_factory.setClassSplitString("\t");

    JobConf job = new JobConf(defaultConf);

    long block_size = localFs.getDefaultBlockSize(workDir);

    LOG.info("default block size: " + (block_size / 1024 / 1024) + "MB");

    // matches the OLR setup on p.269 ---------------
    // stepOffset, decay, and alpha --- describe how the learning rate decreases
    // lambda: amount of regularization
    // learningRate: amount of initial learning rate
    @SuppressWarnings("resource")
    OnlineLogisticRegression learningAlgorithm = new OnlineLogisticRegression(
        20, FEATURES, new L1()).alpha(1).stepOffset(1000).decayExponent(0.9)
        .lambda(3.0e-5).learningRate(20);

    FileInputFormat.setInputPaths(job, workDir);

    // try splitting the file in a variety of sizes
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    Text value = new Text();

    int numSplits = 1;

    InputSplit[] splits = format.getSplits(job, numSplits);

    LOG.info("requested " + numSplits + " splits, splitting: got =        "
        + splits.length);
    LOG.info("---- debug splits --------- ");
    rec_factory.Debug();
    int total_read = 0;

    for (int x = 0; x < splits.length; x++) {

      LOG.info("> Split [" + x + "]: " + splits[x].getLength());

      int count = 0;
      InputRecordsSplit custom_reader = new InputRecordsSplit(job, splits[x]);
      while (custom_reader.next(value)) {
        Vector v = new RandomAccessSparseVector(
            TwentyNewsgroupsRecordFactory.FEATURES);
        int actual = rec_factory.processLine(value.toString(), v);

        String ng = rec_factory.GetNewsgroupNameByID(actual);

        // calc stats ---------

        double mu = Math.min(k + 1, 200);
        double ll = learningAlgorithm.logLikelihood(actual, v);
        averageLL = averageLL + (ll - averageLL) / mu;

        Vector p = new DenseVector(20);
        learningAlgorithm.classifyFull(p, v);
        int estimated = p.maxValueIndex();

        int correct = (estimated == actual ? 1 : 0);
        averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
        learningAlgorithm.train(actual, v);
        k++;
        int bump = bumps[(int) Math.floor(step) % bumps.length];
        int scale = (int) Math.pow(10, Math.floor(step / bumps.length));

        if (k % (bump * scale) == 0) {
          step += 0.25;
          LOG.info(String.format("%10d %10.3f %10.3f %10.2f %s %s", k, ll,
              averageLL, averageCorrect * 100, ng,
              rec_factory.GetNewsgroupNameByID(estimated)));
        }

        learningAlgorithm.close();
        count++;
      }

      LOG.info("read: " + count + " records for split " + x);
      total_read += count;
    } // for each split
    LOG.info("total read across all splits: " + total_read);
    rec_factory.Debug();
  }
}
