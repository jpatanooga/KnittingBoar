package com.cloudera.knittingboar.yarn.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.ConfigFields;
import com.cloudera.knittingboar.yarn.appmaster.ApplicationMaster;

public class TestClient {
  
  private static Log LOG = LogFactory.getLog(TestClient.class);
  
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();
  protected static File testDir;
  protected static String hadoopHome = "/Users/Michael/Downloads/hadoop/hadoop-2.0.0-cdh4.0.1";
  
  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info("Starting up YARN cluster");
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(TestClient.class.getName(),
          1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      
      // Setup yarn-site.xml
      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) 
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");

      testDir = new File(url.getPath()).getParentFile();
      yarnCluster.getConfig().set("yarn.application.classpath", url.getPath() + "," 
//          + hadoopHome + "/etc/hadoop," 
          + hadoopHome + "/share/hadoop/common/*,"
          + hadoopHome + "/share/hadoop/common/lib/*,"
          + hadoopHome + "/share/hadoop/hdfs/*,"
          + hadoopHome + "/share/hadoop/hdfs/lib/*,"
          + hadoopHome + "/share/hadoop/mapreduce/*,"
          + hadoopHome + "/share/hadoop/mapreduce/lib/*,"
          + testDir.getPath() + "," 
          + testDir.getParentFile().getPath() + "/classes");
      
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      yarnCluster.getConfig().writeXml(os);
      os.close();

      // Setup app.properties
      InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties");
      if (is == null) 
        throw new RuntimeException("Could not find 'app.properties' template file in classpath");

      Properties props = new Properties();
      props.load(is);
      props.put(ConfigFields.JAR_PATH, "/dev/null");
      props.put(ConfigFields.APP_JAR_PATH, "/dev/null");
      props.put(ConfigFields.APP_INPUT_PATH, testDir + "/input.dat");
      props.put(ConfigFields.APP_OUTPUT_PATH, testDir + "/output.dat");
      props.put(ConfigFields.YARN_MASTER, "com.cloudera.knittingboar.yarn.appmaster.TestApplicationMaster");
      props.put(ConfigFields.YARN_WORKER, "com.cloudera.knittingboar.yarn.appworker.TestApplicationWorker");
      
      props.store(new FileOutputStream(testDir.getPath() + "/app.properties"), null);
      
      // Input file (really set up a mini HDFS)
      FileOutputStream fos = new FileOutputStream(testDir + "/input.dat");
      fos.write("10\n20\n30\n40\n50\n60\n70\n80\n90\n100".getBytes());
      fos.close();
      
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    } 
  }
  
  @AfterClass
  public static void cleanup() {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }
  
  @Before
  public void setupTest() {
    
  }
  
  @Test
  public void testClient() throws Exception {
    LOG.info("Starting test client...");
    Client client = new Client();
    client.setConf(yarnCluster.getConfig());
    client.run(new String[] { testDir + "/app.properties"});
  }
}