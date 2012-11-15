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

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.TrainLogistic;

import com.cloudera.iterativereduce.ConfigFields;
import com.cloudera.iterativereduce.yarn.client.Client;
import com.google.common.collect.Lists;

public class ModelTrainerCmdLineDriver extends Client {
  
  private static String input_dir = "";
  private static String output_dir = "";
  
  public static void main(String[] args) throws Exception {
    mainToOutput(args, new PrintWriter(System.out, true));
    
    int rc = ToolRunner.run(new Configuration(),
        new ModelTrainerCmdLineDriver(), args);
    
    // Log, because been bitten before on daemon threads; sanity check
    System.out.println("Calling System.exit(" + rc + ")");
    System.exit(rc);
  }
  
  static void mainToOutput(String[] args, PrintWriter output) throws Exception {
    if (parseArgs(args)) {
      
      output.write("Parse:correct");
      
    } // if
  } // mainToOutput
  
  private static boolean parseArgs(String[] args) {
    DefaultOptionBuilder builder = new DefaultOptionBuilder();
    
    Option help = builder.withLongName("help").withDescription(
        "print this list").create();
    
    // Option quiet =
    // builder.withLongName("quiet").withDescription("be extra quiet").create();
    // Option scores =
    // builder.withLongName("scores").withDescription("output score diagnostics during training").create();
    
    ArgumentBuilder argumentBuilder = new ArgumentBuilder();
    Option inputFile = builder
        .withLongName("input")
        .withRequired(true)
        .withArgument(argumentBuilder.withName("input").withMaximum(1).create())
        .withDescription("where to get training data").create();
    
    Option outputFile = builder.withLongName("output").withRequired(true)
        .withArgument(
            argumentBuilder.withName("output").withMaximum(1).create())
        .withDescription("where to get training data").create();
    
    Option features = builder.withLongName("features").withArgument(
        argumentBuilder.withName("numFeatures").withDefault("1000")
            .withMaximum(1).create()).withDescription(
        "the number of internal hashed features to use").create();
    
    // optionally can be { 20Newsgroups, rcv1 }
    Option RecordFactoryType = builder.withLongName("recordFactoryType")
        .withArgument(
            argumentBuilder.withName("recordFactoryType").withDefault(
                "20Newsgroups").withMaximum(1).create()).withDescription(
            "the record vectorization factory to use").create();
    
    Option passes = builder.withLongName("passes").withArgument(
        argumentBuilder.withName("passes").withDefault("2").withMaximum(1)
            .create()).withDescription(
        "the number of times to pass over the input data").create();
    
    Option lambda = builder.withLongName("lambda").withArgument(
        argumentBuilder.withName("lambda").withDefault("1e-4").withMaximum(1)
            .create())
        .withDescription("the amount of coefficient decay to use").create();
    
    Option rate = builder.withLongName("rate").withArgument(
        argumentBuilder.withName("learningRate").withDefault("1e-3")
            .withMaximum(1).create()).withDescription("the learning rate")
        .create();
    
    Option noBias = builder.withLongName("noBias").withDescription(
        "don't include a bias term").create();
    
    Group normalArgs = new GroupBuilder().withOption(help)
        .withOption(inputFile).withOption(outputFile).withOption(
            RecordFactoryType).withOption(passes).withOption(lambda)
        .withOption(rate).withOption(noBias).withOption(features).create();
    
    Parser parser = new Parser();
    parser.setHelpOption(help);
    parser.setHelpTrigger("--help");
    parser.setGroup(normalArgs);
    parser.setHelpFormatter(new HelpFormatter(" ", "", " ", 130));
    CommandLine cmdLine = parser.parseAndHelp(args);
    
    if (cmdLine == null) {
      
      System.out.println("null!");
      return false;
    }
    
    input_dir = getStringArgument(cmdLine, inputFile);
    output_dir = getStringArgument(cmdLine, outputFile);
    
    /*
     * TrainLogistic.inputFile = getStringArgument(cmdLine, inputFile);
     * TrainLogistic.outputFile = getStringArgument(cmdLine, outputFile);
     * 
     * List<String> typeList = Lists.newArrayList(); for (Object x :
     * cmdLine.getValues(types)) { typeList.add(x.toString()); }
     * 
     * List<String> predictorList = Lists.newArrayList(); for (Object x :
     * cmdLine.getValues(predictors)) { predictorList.add(x.toString()); }
     * 
     * lmp = new LogisticModelParameters();
     * lmp.setTargetVariable(getStringArgument(cmdLine, target));
     * lmp.setMaxTargetCategories(getIntegerArgument(cmdLine,
     * targetCategories)); lmp.setNumFeatures(getIntegerArgument(cmdLine,
     * features)); lmp.setUseBias(!getBooleanArgument(cmdLine, noBias));
     * lmp.setTypeMap(predictorList, typeList);
     * 
     * lmp.setLambda(getDoubleArgument(cmdLine, lambda));
     * lmp.setLearningRate(getDoubleArgument(cmdLine, rate));
     * 
     * TrainLogistic.scores = getBooleanArgument(cmdLine, scores);
     * TrainLogistic.passes = getIntegerArgument(cmdLine, passes);
     */
    return true;
  }
  
  public Configuration generateDebugConfigurationObject() {
    
    Configuration c = new Configuration();
    
    // feature vector size
    c.setInt("com.cloudera.knittingboar.setup.FeatureVectorSize", 10000);
    
    c.setInt("com.cloudera.knittingboar.setup.numCategories", 20);
    
    c.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    c.setInt("com.cloudera.knittingboar.setup.NumberPasses", 1);
    
    // local input split path
    c.set("com.cloudera.knittingboar.setup.LocalInputSplitPath",
        "hdfs://127.0.0.1/input/0");
    
    // setup 20newsgroups
    c.set("com.cloudera.knittingboar.setup.RecordFactoryClassname",
        "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");
    
    return c;
    
  }
  
  private void BuildPropertiesFile() throws Exception {
    
    // Setup app.properties
    InputStream is = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("app.properties");
    if (is == null) throw new RuntimeException(
        "Could not find 'app.properties' template file in classpath");
    
    Properties props = new Properties();
    props.load(is);
    props.put(ConfigFields.JAR_PATH, "/dev/null"); // what about these?
    props.put(ConfigFields.APP_JAR_PATH, "/dev/null"); // what about these?
    props.put(ConfigFields.APP_INPUT_PATH, ModelTrainerCmdLineDriver.input_dir);
    props.put(ConfigFields.APP_OUTPUT_PATH,
        ModelTrainerCmdLineDriver.output_dir);
    props.put(ConfigFields.YARN_MASTER,
        "com.cloudera.knittingboar.sgd.iterativereduce.POLRMasterNode");
    props.put(ConfigFields.YARN_WORKER,
        "com.cloudera.knittingboar.sgd.iterativereduce.POLRWorkerNode");
    
    props.put("com.cloudera.knittingboar.setup.FeatureVectorSize", 10000);
    
    props.put("com.cloudera.knittingboar.setup.numCategories", 20);
    
    props.put("com.cloudera.knittingboar.setup.BatchSize", 200);
    
    props.put("com.cloudera.knittingboar.setup.NumberPasses", 1);
    
    // local input split path
    // props.put( "com.cloudera.knittingboar.setup.LocalInputSplitPath",
    // "hdfs://127.0.0.1/input/0" );
    
    // setup 20newsgroups
    props.put("com.cloudera.knittingboar.setup.RecordFactoryClassname",
        "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");
    
    props.store(new FileOutputStream("app.properties"), null);
    
  }
  
  /*
   * public void Train() {
   * 
   * Client client = new Client(); client.setConf(yarnCluster.getConfig());
   * client.run(new String[] { testDir + "/app.properties"});
   * 
   * }
   */

  private static boolean getBooleanArgument(CommandLine cmdLine, Option option) {
    return cmdLine.hasOption(option);
  }
  
  private static String getStringArgument(CommandLine cmdLine, Option inputFile) {
    return (String) cmdLine.getValue(inputFile);
  }
  
}
