package com.cloudera.knittingboar.yarn;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;

/*
 * TODO: fix to more sane method of copying collections (e.g. Collections.addAll
 * 
 */
public class Utils {
  
  private static Log LOG = LogFactory.getLog(Utils.class);

  public static String getWorkerId(WorkerId workerId) {
    return new String(workerId.bytes()).trim();
  }

  public static WorkerId createWorkerId(String name) {
    byte[] buff = new byte[32];
    System.arraycopy(name.getBytes(), 0, buff, 0, name.length());

    return new WorkerId(buff);
  }
  
  public static void mergeConfigs(StartupConfiguration sourceConf,
      Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;

    Map<CharSequence, CharSequence> confMap = sourceConf.getOther();

    if (confMap == null)
      return;

    for (Map.Entry<CharSequence, CharSequence> entry : confMap.entrySet()) {
      destConf.set(entry.getKey().toString(), entry.getValue().toString());
    }
  }

  public static void mergeConfigs(Configuration sourceConf, Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;

    for (Map.Entry<String, String> entry : sourceConf) {
      if (destConf.get(entry.getKey()) == null) {
        destConf.set(entry.getKey(), entry.getValue());
      }
    }
  }
  
  public static void mergeConfigs(Map<CharSequence, CharSequence> sourceConf, Configuration destConf) {
    if (sourceConf == null || destConf == null)
      return;
    
    for(Map.Entry<CharSequence, CharSequence> entry : sourceConf.entrySet()) {
      if (destConf.get(entry.getKey().toString()) == null) {
        destConf.set(entry.getKey().toString(), entry.getValue().toString());
      }
    }
  }
  
  public static void mergeConfigs(Map<CharSequence, CharSequence> sourceConf, StartupConfiguration destConf) {
    if (sourceConf == null || destConf == null)
      return;
    
    Map<CharSequence, CharSequence> confMap = destConf.getOther();
    
    if (confMap == null)
      confMap = new HashMap<CharSequence, CharSequence>();
    
    for (Map.Entry<CharSequence, CharSequence> entry : sourceConf.entrySet()) {
      if (! confMap.containsKey(entry.getKey()))
        confMap.put(entry.getKey(), entry.getValue());
    }
    
    destConf.setOther(confMap);
  }
  
  private static void copyToFs(Configuration conf, String local, String remote) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path src = new Path(local);
    Path dst = fs.makeQualified(new Path(remote));

    LOG.debug("Copying to filesystem, src=" + src.toString() + ", dst="
        +  dst);

    fs.copyFromLocalFile(false, true, src, dst);
  }
  
  public static String getFileName(String f) {
    return new Path(f).getName();
  }
  
  public static String getAppTempDirectory(ApplicationId appId, String appName) {
    return "/tmp/" + appName + "/" + appId;
  }

  /*
   * Copy local file to a named-file in remote FS
   */
  public static void copyLocalResourceToFs(String src, String dst, Configuration conf,
      ApplicationId appId, String appName) throws IOException {
    
    String tempDir = getAppTempDirectory(appId, appName);
    LOG.debug("Using temporary directory: " + tempDir);
    
    copyToFs(conf, src, tempDir + "/" + dst);
  }
  
  /*
   * Copy batch of resources based on Properties. Destination name will be same.
   */
  public static void copyLocalResourcesToFs(Properties props, Configuration conf, 
      ApplicationId appId, String appName) throws IOException {
    
    String tempDir = getAppTempDirectory(appId, appName);
    String file;
    LOG.debug("Using temporary directory: " + tempDir);
    
    // Our JAR
    file = props.getProperty(ConfigFields.JAR_PATH);
    copyToFs(conf, file, tempDir + "/" + getFileName(file));
    
    // User JAR
    file = props.getProperty(ConfigFields.APP_JAR_PATH);
    copyToFs(conf, file, tempDir + "/" + getFileName(file));
    
    // Libs
    for (String f : props.getProperty(ConfigFields.APP_LIB_PATH).split(",")) {
      if (f.trim().equals(""))
        continue;
      
      copyToFs(conf, f, tempDir + "/" + getFileName(f));
    }
  }

  
  public static String getPathForResource(String loc, 
      ApplicationId appId, String appName) {

    return getAppTempDirectory(appId, appName) + "/" + getFileName(loc);
  }
  
  public static Map<String, LocalResource> getLocalResourcesForApplication(
      Configuration conf, ApplicationId appId, String appName,
      Properties props, LocalResourceVisibility visibility) throws IOException {
    
    List<String> resources = new ArrayList<String>();
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    
    resources.add(getPathForResource(props.getProperty(ConfigFields.JAR_PATH), appId, appName)); // Our app JAR
    resources.add(getPathForResource(props.getProperty(ConfigFields.APP_JAR_PATH), appId, appName)); // User app JAR
    resources.add(getPathForResource(ConfigFields.APP_CONFIG_FILE, appId, appName)); // Our application configuration
    resources.add(getPathForResource("log4j.properties", appId, appName));
    // Libs
    String libs = props.getProperty(ConfigFields.APP_LIB_PATH);
    if (libs != null && !libs.isEmpty()) {
      for (String s : libs.split(",")) {
        resources.add(getPathForResource(s, appId, appName));
      }
    }

    FileSystem fs = FileSystem.get(conf);
    Path fsPath;
    FileStatus fstat;
    
    // Convert to local resource list
    for (String resource : resources) {
      fsPath = new Path(resource);
      fstat = fs.getFileStatus(fsPath); 
      LOG.debug("Processing local resource=" + fstat.getPath());
      
      LocalResource localResource = Records.newRecord(LocalResource.class);
      localResource.setResource(ConverterUtils.getYarnUrlFromPath(fstat.getPath()));
      localResource.setSize(fstat.getLen());
      localResource.setTimestamp(fstat.getModificationTime());
      localResource.setVisibility(visibility);
      localResource.setType(LocalResourceType.FILE);

      localResources.put(fsPath.getName(), localResource);
    }

    return localResources;
  }
  
  public static Map<String, String> getEnvironment(Configuration conf, Properties props) {
    Map<String, String> env = new LinkedHashMap<String, String>();
    
    // First lets get any arbitrary environment variables in the config
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = ((String)entry.getKey());
      if (key.startsWith("env.")) {
        String envname = key.substring(4);
        String envvalue = (String)entry.getValue();
        env.put(envname, envvalue);
        
        LOG.debug("Adding " + envname + "=" + envvalue + " to envrionment");
      }
    }
    
    StringBuffer sb = new StringBuffer("${CLASSPATH}:./*:");
    String path;
    
    // Probably redundant
    // Our Jar
    path = Utils.getFileName(props.getProperty(ConfigFields.JAR_PATH));
    sb.append("./").append(path).append(":");
    
    // App Jar
    path = Utils.getFileName(props.getProperty(ConfigFields.APP_JAR_PATH));
    sb.append("./").append(path).append(":");

    // Any libraries we may have copied over?
    for (String c : props.getProperty(ConfigFields.APP_LIB_PATH).split(",")) {
      if (c.trim().equals(""))
        continue;
      
      path = Utils.getFileName(c.trim());
      sb.append("./").append(path).append(":");
    }
    
    // Generic
    for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH)
        .split(",")) {
      sb.append(c.trim());
      sb.append(':');
    }

    if (props.get(ConfigFields.CLASSPATH_EXTRA) != null) {
      sb.append(props.get(ConfigFields.CLASSPATH_EXTRA)).append(":");
    }
    
    sb.append("./log4j.properties");

    LOG.debug("Adding CLASSPATH=" + sb.toString() + " to environment");
    env.put("CLASSPATH", sb.toString());
    
    return env;
  }
  
  public static List<String> getWorkerCommand(Configuration conf,
      Properties props, String masterAddress, String workerId) {
    
    List<String> commands = new ArrayList<String>();
    String command = props.getProperty(ConfigFields.YARN_WORKER);
    String args = props.getProperty(props.getProperty(ConfigFields.YARN_WORKER));
    
    command += " --master-addr " + masterAddress;
    command += " --worker-id " + workerId;
    
    StringBuffer cmd = getCommandsBase(conf, props, command, args);
    commands.add(cmd.toString());

    return commands;
    
  }
  
  public static List<String> getMasterCommand(Configuration conf, Properties props) {
    List<String> commands = new ArrayList<String>();
    String command = props.getProperty(ConfigFields.YARN_MASTER);
    String args = props.getProperty(ConfigFields.YARN_MASTER_ARGS);
    
    StringBuffer cmd = getCommandsBase(conf, props, command, args);
    commands.add(cmd.toString());
    
    return commands;
  }
  
  private static StringBuffer getCommandsBase(Configuration conf, Properties props, String command,
      String args) {
    
    StringBuffer sb = new StringBuffer();
    
    sb.append("java ");
    sb.append("-Xmx").append(props.getProperty(ConfigFields.YARN_MEMORY, "512")).append("m ");

    if (args != null)
      sb.append(" ").append(args).append(" ");

    // Actual command
    sb.append(command);

    sb.append(" 1> ")
        .append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append(Path.SEPARATOR)
        .append(ApplicationConstants.STDOUT);

    sb.append(" 2> ")
        .append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append(Path.SEPARATOR)
        .append(ApplicationConstants.STDERR);
    
    return sb;
  }
  
  public static ResourceRequest createResourceRequest(String host, int amount, int memory) {
    ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);
    rsrcRequest.setHostName(host);

    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    rsrcRequest.setPriority(pri);

    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    rsrcRequest.setCapability(capability);
    
    rsrcRequest.setNumContainers(amount);
    
    LOG.debug("Created a resource request"
        + ", host=" + rsrcRequest.getHostName()
        + ", memory=" + rsrcRequest.getCapability().getMemory() 
        + ", amount=" + rsrcRequest.getNumContainers()
        + ", priority=" + rsrcRequest.getPriority().getPriority());

    return rsrcRequest;
  }
}