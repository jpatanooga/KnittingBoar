package com.cloudera.knittingboar.yarn.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.cloudera.knittingboar.yarn.ConfigFields;
import com.cloudera.knittingboar.yarn.ResourceManagerHandler;
import com.cloudera.knittingboar.yarn.Utils;

public class Client extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(Client.class);
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1)
      LOG.info("No configuration file specified, using default ("
          + ConfigFields.DEFAULT_CONFIG_FILE + ")");
    
    String configFile = (args.length < 1) ? ConfigFields.DEFAULT_CONFIG_FILE : args[0];
    Properties props = new Properties();
        
    try {
      FileInputStream fis = new FileInputStream(configFile);
      props.load(fis);
    } catch (FileNotFoundException ex) {
      throw ex; // TODO: be nice
    } catch (IOException ex) {
      throw ex; // TODO: be nice
    }
    
    // Make sure we have some bare minimums
    ConfigFields.validateConfig(props);
    
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        LOG.debug(entry.getKey() + "=" + entry.getValue());
      }
    }

    // TODO: make sure input file(s), libs, etc. actually exist!
    // Ensure our input path exists
    Configuration conf = getConf();
    Path p = new Path(props.getProperty(ConfigFields.APP_INPUT_PATH));
    FileSystem fs = p.getFileSystem(conf);
    
    if (!fs.exists(p))
      throw new FileNotFoundException("Unable to find " + p.toString()
          + " in filesystem " + fs.getUri());

    // Connect
    ResourceManagerHandler rmHandler = new ResourceManagerHandler(conf, null);
    rmHandler.getClientResourceManager();

    ApplicationId appId = rmHandler.getApplicationId(); // Our AppId
    String appName = props.getProperty(ConfigFields.APP_NAME); // Our name
    
    Utils.copyLocalResourcesToFs(props, conf, appId, appName); // Local resources
    Utils.copyLocalResourceToFs(configFile, ConfigFields.APP_CONFIG_FILE, conf,
        appId, appName); // Config file

    // Create our context
    List<String> commands = Utils.getMasterCommand(props);
    Map<String, LocalResource> localResources = Utils
        .getLocalResourcesForApplication(conf, appId, appName, props,
            LocalResourceVisibility.APPLICATION);

    // Submit app
    rmHandler.submitApplication(appId, appName, 
        localResources, commands,
        Integer.parseInt(props.getProperty(ConfigFields.YARN_MEMORY, "512")));    

    // Wait for app to complete
    while (true) {
      Thread.sleep(2000);
      
      ApplicationReport report = rmHandler.getApplicationReport(appId);
      
      LOG.info("Got applicaton report for"
          + ", appId=" + appId.getId()
          + ", clientToken=" + report.getClientToken() 
          + ", amDiag=" + report.getDiagnostics()
          + ", masterHost=" + report.getHost()
          + ", queue=" + report.getQueue()
          + ", masterRpcPort=" + report.getRpcPort()
          + ", startTime=" + report.getStartTime()
          + ", state=" + report.getYarnApplicationState().toString()
          + ", finalState=" + report.getFinalApplicationStatus().toString()
          + ", trackingUrl=" + report.getTrackingUrl()
          + ", user=" + report.getUser());
      
      if (YarnApplicationState.FINISHED == report.getYarnApplicationState()) {
        if (FinalApplicationStatus.SUCCEEDED == report.getFinalApplicationStatus()) {
          LOG.info("Application completed succesfully.");
          return 0;
        } else {
          LOG.info("Application completed with en error.");
          return -1;
        }
      } else if (YarnApplicationState.FAILED == report.getYarnApplicationState() ||
          YarnApplicationState.KILLED == report.getYarnApplicationState()) {

        LOG.info("Application completed with a failed or killed state.");
        return -1; 
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Client(), args);
    
    // Log, because been bitten before on daemon threads; sanity check
    LOG.debug("Calling System.exit(" + rc + ")");
    System.exit(rc);
  }
}
