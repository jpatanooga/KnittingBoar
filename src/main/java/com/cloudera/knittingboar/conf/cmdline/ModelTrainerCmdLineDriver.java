package com.cloudera.knittingboar.conf.cmdline;

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.TrainLogistic;

import com.google.common.collect.Lists;

public class ModelTrainerCmdLineDriver {

  public static void main(String[] args) throws Exception {
    mainToOutput(args, new PrintWriter(System.out, true));
  }
  
  static void mainToOutput(String[] args, PrintWriter output) throws Exception {
    if (parseArgs(args)) {
      
      output.write("Parse:correct");

    } // if
  } // mainToOutput

  private static boolean parseArgs(String[] args) {
    DefaultOptionBuilder builder = new DefaultOptionBuilder();

    Option help = builder.withLongName("help").withDescription("print this list").create();

    //Option quiet = builder.withLongName("quiet").withDescription("be extra quiet").create();
    //Option scores = builder.withLongName("scores").withDescription("output score diagnostics during training").create();

    ArgumentBuilder argumentBuilder = new ArgumentBuilder();
    Option inputFile = builder.withLongName("input")
            .withRequired(true)
            .withArgument(argumentBuilder.withName("input").withMaximum(1).create())
            .withDescription("where to get training data")
            .create();

    Option outputFile = builder.withLongName("output")
            .withRequired(true)
            .withArgument(argumentBuilder.withName("output").withMaximum(1).create())
            .withDescription("where to get training data")
            .create();

    Option features = builder.withLongName("features")
            .withArgument(
                    argumentBuilder.withName("numFeatures")
                            .withDefault("1000")
                            .withMaximum(1).create())
            .withDescription("the number of internal hashed features to use")
            .create();

    
    // optionally can be { 20Newsgroups, rcv1 }
    Option RecordFactoryType = builder.withLongName("recordFactoryType")
            .withArgument(
            argumentBuilder.withName("recordFactoryType")
                    .withDefault("20Newsgroups")
                    .withMaximum(1).create())
    .withDescription("the record vectorization factory to use")
    .create();    
    
    
    Option passes = builder.withLongName("passes")
            .withArgument(
                    argumentBuilder.withName("passes")
                            .withDefault("2")
                            .withMaximum(1).create())
            .withDescription("the number of times to pass over the input data")
            .create();

    Option lambda = builder.withLongName("lambda")
            .withArgument(argumentBuilder.withName("lambda").withDefault("1e-4").withMaximum(1).create())
            .withDescription("the amount of coefficient decay to use")
            .create();

    Option rate = builder.withLongName("rate")
            .withArgument(argumentBuilder.withName("learningRate").withDefault("1e-3").withMaximum(1).create())
            .withDescription("the learning rate")
            .create();

    Option noBias = builder.withLongName("noBias")
            .withDescription("don't include a bias term")
            .create();

    Group normalArgs = new GroupBuilder()
            .withOption(help)
            .withOption(inputFile)
            .withOption(outputFile)
            .withOption(RecordFactoryType)
            .withOption(passes)
            .withOption(lambda)
            .withOption(rate)
            .withOption(noBias)
            .withOption(features)
            .create();

    Parser parser = new Parser();
    parser.setHelpOption(help);
    parser.setHelpTrigger("--help");
    parser.setGroup(normalArgs);
    parser.setHelpFormatter(new HelpFormatter(" ", "", " ", 130));
    CommandLine cmdLine = parser.parseAndHelp(args);

    if (cmdLine == null) {
      
      System.out.println( "null!" );
      return false;
    }
    
    
    String strInput = getStringArgument(cmdLine, inputFile);
    
    
/*
    TrainLogistic.inputFile = getStringArgument(cmdLine, inputFile);
    TrainLogistic.outputFile = getStringArgument(cmdLine, outputFile);

    List<String> typeList = Lists.newArrayList();
    for (Object x : cmdLine.getValues(types)) {
      typeList.add(x.toString());
    }

    List<String> predictorList = Lists.newArrayList();
    for (Object x : cmdLine.getValues(predictors)) {
      predictorList.add(x.toString());
    }

    lmp = new LogisticModelParameters();
    lmp.setTargetVariable(getStringArgument(cmdLine, target));
    lmp.setMaxTargetCategories(getIntegerArgument(cmdLine, targetCategories));
    lmp.setNumFeatures(getIntegerArgument(cmdLine, features));
    lmp.setUseBias(!getBooleanArgument(cmdLine, noBias));
    lmp.setTypeMap(predictorList, typeList);

    lmp.setLambda(getDoubleArgument(cmdLine, lambda));
    lmp.setLearningRate(getDoubleArgument(cmdLine, rate));

    TrainLogistic.scores = getBooleanArgument(cmdLine, scores);
    TrainLogistic.passes = getIntegerArgument(cmdLine, passes);
*/
    return true;
  }
  
  public void Train() {
    
    
    
  }
  

  private static boolean getBooleanArgument(CommandLine cmdLine, Option option) {
    return cmdLine.hasOption(option);
  }

  private static String getStringArgument(CommandLine cmdLine, Option inputFile) {
    return (String) cmdLine.getValue(inputFile);
  }
  
  
}
