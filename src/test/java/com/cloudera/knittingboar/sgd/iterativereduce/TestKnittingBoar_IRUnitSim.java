package com.cloudera.knittingboar.sgd.iterativereduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.iterativereduce.irunit.IRUnitDriver;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRMasterNode;
import com.cloudera.knittingboar.sgd.iterativereduce.POLRWorkerNode;
import com.cloudera.knittingboar.utils.DataUtils;
import com.cloudera.knittingboar.utils.DatasetConverter;


public class TestKnittingBoar_IRUnitSim  extends TestCase {

  
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
  
  //private static Path workDir = new Path(System.getProperty("test.build.data", "/Users/jpatterson/Downloads/datasets/20news-kboar/train4/"));  
  private static Path workDir20NewsLocal = new Path(new Path("/tmp"), "Dataset20Newsgroups");
  private static File unzipDir = new File( workDir20NewsLocal + "/20news-bydate");
  private static String strKBoarTrainDirInput = "" + unzipDir.toString() + "/KBoar-train/";

  
  public void setupResources() throws IOException {

    File file20News = DataUtils.getTwentyNewsGroupDir();
    
    DatasetConverter.ConvertNewsgroupsFromSingleFiles( DataUtils.get20NewsgroupsLocalDataLocation() + "/20news-bydate-train/", strKBoarTrainDirInput, 6000);
    
    
  }
  
  public void testIRUnit_POLR() throws IOException {
    
    System.out.println( "Starting: testIRUnit_POLR" );
    
    setupResources();
    
    String[] props = {
        "app.iteration.count",
    "com.cloudera.knittingboar.setup.FeatureVectorSize",
    "com.cloudera.knittingboar.setup.numCategories",
    "com.cloudera.knittingboar.setup.RecordFactoryClassname"
    };
    
    IRUnitDriver polr_driver = new IRUnitDriver("src/test/resources/app_unit_test.properties", props );
    
    polr_driver.SetProperty("app.input.path", strKBoarTrainDirInput);
    
    polr_driver.Setup();
    polr_driver.SimulateRun();
    
    System.out.println("\n\nComplete...");

    POLRMasterNode IR_Master = (POLRMasterNode)polr_driver.getMaster();
    
    Path out = new Path("/tmp/IR_Model_0.model");
    FileSystem fs = out.getFileSystem(defaultConf);
    FSDataOutputStream fos = fs.create(out);

    //LOG.info("Writing master results to " + out.toString());
    IR_Master.complete(fos);

    fos.flush();
    fos.close();        
    
    System.out.println("\n\nModel Saved: /tmp/IR_Model_0.model" );
    
  }
    
  
}
