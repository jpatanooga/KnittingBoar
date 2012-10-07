package com.cloudera.knittingboar.conf.cmdline;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;

import com.cloudera.knittingboar.utils.DatasetConverter;

public class DataConverterCmdLineDriver {

  private static String strInputFile;
  private static String strOutputFile;
  private static String strrecordsPerBlock;
  
  
  public static void main(String[] args) throws Exception {
    mainToOutput(args, new PrintWriter(System.out, true));
  }
  
  static void mainToOutput(String[] args, PrintWriter output) throws Exception {
    if (!parseArgs(args)) {
      
      output.write("Parse:Incorrect");
       return;
    } // if
    
    output.write("Parse:correct");
    
    
    int shard_rec_count = Integer.parseInt(strrecordsPerBlock);
    
    int count = DatasetConverter.ConvertNewsgroupsFromSingleFiles( strInputFile, strOutputFile, shard_rec_count );

    output.write( "Total Records Converted: " + count );
    
    
    
    
    
    
  } // mainToOutput

  private static boolean parseArgs(String[] args) throws IOException {
    DefaultOptionBuilder builder = new DefaultOptionBuilder();

    Option help = builder.withLongName("help").withDescription("print this list").create();

    //Option quiet = builder.withLongName("quiet").withDescription("be extra quiet").create();
    //Option scores = builder.withLongName("scores").withDescription("output score diagnostics during training").create();

    ArgumentBuilder argumentBuilder = new ArgumentBuilder();
    Option inputFileOption = builder.withLongName("input")
            .withRequired(true)
            .withArgument(argumentBuilder.withName("input").withMaximum(1).create())
            .withDescription("where to get input data")
            .create();

    Option outputFileOption = builder.withLongName("output")
            .withRequired(true)
            .withArgument(argumentBuilder.withName("output").withMaximum(1).create())
            .withDescription("where to write output data")
            .create();

    Option recordsPerBlockOption = builder.withLongName("recordsPerBlock")
            .withArgument(
                    argumentBuilder.withName("recordsPerBlock")
                            .withDefault("20000")
                            .withMaximum(1).create())
            .withDescription("the number of records per output file shard to write")
            .create();

    
    // optionally can be { 20Newsgroups, rcv1 }
    Option RecordFactoryType = builder.withLongName("datasetType")
            .withArgument(
            argumentBuilder.withName("recordFactoryType")
                    .withDefault("20Newsgroups")
                    .withMaximum(1).create())
    .withDescription("the type of dataset to convert")
    .create();    
    
/*    
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
*/
    
    Group normalArgs = new GroupBuilder()
            .withOption(help)
            .withOption(inputFileOption)
            .withOption(outputFileOption)
            .withOption(recordsPerBlockOption)
            .withOption(RecordFactoryType)
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
    
    // "/Users/jpatterson/Downloads/datasets/20news-bydate/20news-bydate-train/"
    strInputFile = getStringArgument(cmdLine, inputFileOption);
    
    // "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/"
    strOutputFile = getStringArgument(cmdLine, outputFileOption);
    
    strrecordsPerBlock = getStringArgument(cmdLine, recordsPerBlockOption);

    return true;
  }
  

  private static boolean getBooleanArgument(CommandLine cmdLine, Option option) {
    return cmdLine.hasOption(option);
  }

  private static String getStringArgument(CommandLine cmdLine, Option inputFile) {
    return (String) cmdLine.getValue(inputFile);
  }
  
}