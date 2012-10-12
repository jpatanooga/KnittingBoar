package com.cloudera.knittingboar.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.aspectj.lang.annotation.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClient {
  
  private static Log LOG = LogFactory.getLog(TestClient.class);
  
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();
  
  @BeforeClass
  public static void setUp() {
    LOG.info("Starting up YARN cluster");
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(TestClient.class.getName(),
          1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
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
  
  @Test
  public void testClient() throws Exception {
    Client client = new Client();
    client.setConf(yarnCluster.getConfig());
    client.run(new String[] {});
  }
}

