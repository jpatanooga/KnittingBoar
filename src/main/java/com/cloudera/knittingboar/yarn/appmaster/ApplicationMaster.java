package com.cloudera.knittingboar.yarn.appmaster;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.cloudera.knittingboar.yarn.ConfigFields;
import com.cloudera.knittingboar.yarn.ContainerManagerHandler;
import com.cloudera.knittingboar.yarn.ResourceManagerHandler;
import com.cloudera.knittingboar.yarn.Updateable;
import com.cloudera.knittingboar.yarn.Utils;
import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;
import com.cloudera.knittingboar.yarn.avro.generated.FileSplit;
import com.cloudera.knittingboar.yarn.avro.generated.StartupConfiguration;
import com.cloudera.knittingboar.yarn.avro.generated.WorkerId;

/*
 * Future YARN entry point
 */
public class ApplicationMaster<T extends Updateable> extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  
  private String masterHost;
  private int masterPort;
  private InetSocketAddress masterAddr;
  private ComputableMaster<T> masterComputable;
  private Class<T> masterUpdateable;
  private int batchSize;
  private int iterationCount;
  private Map<CharSequence, CharSequence> appConfig;
  
  private Configuration conf;
  private ContainerId containerId;
  private Properties props;
  private enum ReturnCode { 
    OK(0), MASTER_ERROR(-1), CONTAINER_ERROR(1);

    private int code;
    
    private ReturnCode(int code) {
      this.code = code;
    }
    
    public int getCode() {
      return code;
    }
  }

  public ApplicationMaster(
      ComputableMaster<T> computableMaster,
      Class<T> updatable) throws FileNotFoundException,
      IOException {
    
    this(9999, computableMaster, updatable);
  }
  
  public ApplicationMaster(int port,
      ComputableMaster<T> computableMaster,
      Class<T> updatable) throws FileNotFoundException,
      IOException {
    
    masterHost = InetAddress.getLocalHost().getHostName();   
    masterPort = port;
    masterAddr = new InetSocketAddress(masterHost, masterPort);
    masterComputable = computableMaster;
    masterUpdateable = updatable;
    
    conf = getConf();
    props = new Properties();
    props.load(new FileInputStream(ConfigFields.APP_CONFIG_FILE)); // Should be in ./ - as the Client should've shipped it
    containerId = ConverterUtils.toContainerId(System
        .getenv(ApplicationConstants.AM_CONTAINER_ID_ENV));
    
    batchSize = Integer.parseInt(props.getProperty(ConfigFields.APP_BATCH_SIZE, "200"));
    iterationCount = Integer.parseInt(props.getProperty(ConfigFields.APP_ITERATION_COUNT, "1"));
    
    // Copy all properties into appConfig to be passed down to workers
    for (Map.Entry<Object, Object> prop : props.entrySet()) {
      appConfig.put((String)prop.getKey(), (String)prop.getValue());
    }
  }
  
  class ConfigurationTuple {
    private String host;
    private String workerId;
    private StartupConfiguration config;
    
    public ConfigurationTuple(String host, String workerId, StartupConfiguration config) {
      this.host = host;
      this.workerId = workerId;
      this.config = config;
    }
    
    public String getHost() {
      return host;
    }
    
    public String getWorkerId() {
      return workerId;
    }
    
    public StartupConfiguration getConfig() {
      return config;
    }
  }
  
  // TODO: cache this!
  private Set<ConfigurationTuple> getConfigurationTuples() throws IOException {
    Path p = new Path(props.getProperty(ConfigFields.APP_INPUT_PATH));
    FileSystem fs = p.getFileSystem(conf);
    FileStatus f = fs.getFileStatus(p);
    BlockLocation[] bl = fs.getFileBlockLocations(p, 0, f.getLen());
    Set<ConfigurationTuple> configTuples = new HashSet<ConfigurationTuple>();
    int workerId = 0;
    
    for (BlockLocation b : bl) {
      FileSplit split = FileSplit.newBuilder().setPath(p.toString())
          .setOffset(b.getOffset()).setLength(b.getLength()).build();
      
      StartupConfiguration config = StartupConfiguration.newBuilder()
          .setBatchSize(batchSize)
          .setIterations(iterationCount)
          .setOther(appConfig)
          .setSplit(split)
          .build();
      
      String wid = "worker-" + workerId;
      ConfigurationTuple tuple = new ConfigurationTuple(b.getHosts()[0], wid, config);
      
      configTuples.add(tuple);
      workerId++;
    }

    return configTuples;
  }
  
  private Map<WorkerId, StartupConfiguration> getMasterStartupConfiguration(Set<ConfigurationTuple> configTuples) {
    Map<WorkerId, StartupConfiguration> startupConfig = new HashMap<WorkerId, StartupConfiguration>();
    
    for (ConfigurationTuple tuple : configTuples) {
      WorkerId wid = Utils.createWorkerId(tuple.getHost());
      startupConfig.put(wid, tuple.getConfig());
    }
    
    return startupConfig;
  }

  private Map<String, Integer> getNumberContainersHostMapping(
      Set<ConfigurationTuple> configTuples) {
    
    Map<String, Integer> containerHostMapping = new HashMap<String, Integer>();
    
    for (ConfigurationTuple tuple : configTuples) {
      int  count = containerHostMapping.get(tuple.getHost());
      containerHostMapping.put(tuple.getHost(), count++);
    }
    
    return containerHostMapping;
  }
  
  private List<ResourceRequest> getRequestedContainersList(Set<ConfigurationTuple> configTuples) {
    
    Map<String, Integer> numberContainerHostsMapping =  getNumberContainersHostMapping(configTuples);
    List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
    int memory = Integer.parseInt(props.getProperty(ConfigFields.YARN_MEMORY, "512"));
    
    for (ConfigurationTuple tuple : configTuples) {
      ResourceRequest request = Utils.createResourceRequest(tuple.getHost(),
          numberContainerHostsMapping.get(tuple.getHost()), memory);
      requestedContainers.add(request);
    }
    
    return requestedContainers;
  }
  
  private List<Thread> launchContainers(Set<ConfigurationTuple> configTuples,
      List<Container> allocatedContainers) {
    
    List<Thread> launchThreads = new ArrayList<Thread>();
    Iterator<Container> ic = allocatedContainers.iterator();

    while(ic.hasNext()) {
      Container container = ic.next();
      Iterator<ConfigurationTuple> ict = configTuples.iterator();

      while (ict.hasNext()) {
        ConfigurationTuple tuple = ict.next();
        
        if(tuple.getHost().equals(container.getNodeId().getHost())) {
          LaunchContainerRunnabble runnable = new LaunchContainerRunnabble(tuple.getWorkerId(), container);
          Thread launchThread = new Thread(runnable);
          
          launchThreads.add(launchThread);
          launchThread.start();
          
          ict.remove();
          ic.remove();
          break;
        }
      }
    }
    
    // If we have leftovers, we don't have data-local assignments
    if (allocatedContainers.size() > 0) {
      ic = allocatedContainers.iterator();
      Iterator<ConfigurationTuple> ict = configTuples.iterator();
      
      while (ic.hasNext() && ict.hasNext()) {
        Container container = ic.next();
        ConfigurationTuple tuple = ict.next();
        
        LaunchContainerRunnabble runnable = new LaunchContainerRunnabble(tuple.getWorkerId(), container);
        Thread launchThread = new Thread(runnable);
        
        launchThreads.add(launchThread);
        launchThread.start();
        
        ic.remove();
        ict.remove();
      }
    }
    
    return launchThreads;
  }
  
  private class LaunchContainerRunnabble implements Runnable {
    String workerId;
    Container container;
    ContainerManagerHandler cmHandler;
    
    public LaunchContainerRunnabble(String workerId, Container container) {
      this.workerId = workerId;
      this.container = container;
    }
    
    @Override
    public void run() {
      cmHandler = new ContainerManagerHandler(conf, container);

      // Connect
      cmHandler.getContainerManager();
      
      // Get the local resources
      try {
        Map<String, LocalResource> localResources = Utils
            .getLocalResourcesForApplication(conf, null, workerId, props,
                LocalResourceVisibility.APPLICATION);
        List<String> commands = Utils.getWorkerCommand(props, 
            masterHost + ":" + masterPort, workerId);

        // Start
        cmHandler.startContainer(commands, localResources);
        
        // Get status
        cmHandler.getContainerStatus();
      //} catch (URISyntaxException ex) { // Getting URI for local file, fatal
      } catch (YarnRemoteException ex) { // Container status, fatalish
      } catch (IOException ex) { // Starting container, fatal
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    // Our own RM Handler
    ResourceManagerHandler rmHandler = new ResourceManagerHandler(conf, containerId);
    // Connect
    rmHandler.getAMResourceManager();
    // Register
    try {
      rmHandler.registerApplicationMaster(masterHost, masterPort);
    } catch (YarnRemoteException ex) {
      LOG.error(
          "Error encountered while trying to register application master", ex);
      return ReturnCode.MASTER_ERROR.getCode();
    }

    // Get file splits, configuration, etc.
    Set<ConfigurationTuple> configTuples;
    try {
      configTuples = getConfigurationTuples();
    } catch (IOException ex) {
      LOG.error("Error encountered while trying to generate configurations", ex);
      return ReturnCode.MASTER_ERROR.getCode();
    }
    Map<WorkerId, StartupConfiguration> startupConf = getMasterStartupConfiguration(configTuples);

    // Initial containers we want, based off of the FileSplit's
    List<ResourceRequest> requestedContainers = getRequestedContainersList(configTuples);
    List<ContainerId> releasedContainers = new ArrayList<ContainerId>();

    // Send an initial allocation request
    List<Container> allocatedContainers;
    try {
      allocatedContainers = rmHandler.allocateRequest(
          requestedContainers, releasedContainers).getAllocatedContainers();
    } catch (YarnRemoteException ex) {
      LOG.error("Encountered an error while trying to allocate containers", ex);
      return ReturnCode.MASTER_ERROR.getCode();
    }
    
    final int numContainers = configTuples.size();

    // Make sure we got all our containers, or else bail
    if (allocatedContainers.size() != numContainers) {
      LOG.info("Unable to get requried number of containers, will not continue"
          + ", needed=" + numContainers
          + ", allocated=" + allocatedContainers.size());
      
      requestedContainers.clear(); // We don't want new containers!
      
      // Add containers into released list
      for (Container c : allocatedContainers) {
        releasedContainers.add(c.getId());
      }
      
      // Release containers
      try {
        rmHandler.allocateRequest(requestedContainers, releasedContainers);
      } catch (YarnRemoteException ex) {
        LOG.warn(
            "Encountered an error while trying to release unwanted containers",
            ex);
      }
      
      // bail
      return ReturnCode.MASTER_ERROR.getCode();
    }
   
    /*
    public ApplicationMasterService(InetSocketAddress masterAddr,
        HashMap<WorkerId, StartupConfiguration> workers,
        ComputableMaster<T> computable, Class<T> updatable,
        Map<String, String> appConf, Configuration conf) {
    */
    
    // Launch our worker process, as we now expect workers to actally do something
    ApplicationMasterService<T> masterService = new ApplicationMasterService<T>(
        masterAddr, startupConf, masterComputable, masterUpdateable, appConfig,
        conf);
    
    ExecutorService executor = Executors.newSingleThreadExecutor();
    FutureTask<Integer> masterThread = new FutureTask<Integer>(masterService);
    executor.submit(masterThread);
    
    // We got the number of containers we wanted, let's launch them
    List<Thread> launchThreads = launchContainers(configTuples, allocatedContainers);
    
    // Use an empty list for heartbeat purposes
    requestedContainers.clear();

    // Some local counters. Do we really need Atomic?
    AtomicInteger numCompletedContainers = new AtomicInteger();
    AtomicInteger numFailedContainers = new AtomicInteger();
    
    // Go into run-loop waiting for containers to finish, also our heartbeat
    while (numCompletedContainers.get() < numContainers) {
      // Don't pound the RM
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        LOG.warn("Interrupted while waiting on completed containers", ex);
        return ReturnCode.MASTER_ERROR.getCode();
      }
      
      // Heartbeat, effectively
      List<ContainerStatus> completedContainers;
      
      try {
        completedContainers = rmHandler.allocateRequest(
          requestedContainers, releasedContainers)
          .getCompletedContainersStatuses();
      } catch (YarnRemoteException ex) {
        LOG.warn(
            "Encountered an error while trying to heartbeat to resource manager",
            ex);
        
        continue; // Nothing to report, probably an error / endless loop
      }
      
      for (ContainerStatus cs : completedContainers) {
        int exitCode = cs.getExitStatus();
        if (exitCode != 0) {
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
        } else {
          numCompletedContainers.incrementAndGet();
        }
      }
    }

    // All containers have completed
    // Wait for launch threads to complete (this shouldn't really happen)
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(1000);
      } catch (InterruptedException ex) {
        LOG.warn("Interrupted while waiting for Launcher threads to complete",
            ex);
      }
    }
    
    // Write results to file
    Path out = new Path(props.getProperty(ConfigFields.APP_OUTPUT_PATH));
    FileSystem fs = out.getFileSystem(conf);
    FSDataOutputStream fos = fs.create(out);
    
    LOG.info("Writing master results to " + out.toString());
    masterComputable.complete(fos);
    
    fos.flush();
    fos.close();
    
    // Ensure that our master service has completed as well
    int masterExit = masterThread.get();
    LOG.info("Master services completed with exitCode=" + masterExit);
    
    // Application finished
    ReturnCode rc = (numFailedContainers.get() == 0) ? ReturnCode.OK : ReturnCode.CONTAINER_ERROR;
    
    try {
      if (numFailedContainers.get() == 0) {
        rmHandler.finishApplication("Completed succesfully", FinalApplicationStatus.SUCCEEDED);
      } else {
        String diag = "Completed with " + numFailedContainers.get()
            + " failed cotainers";
        rmHandler.finishApplication(diag, FinalApplicationStatus.FAILED);
      }
    } catch (YarnRemoteException ex) {
      LOG.warn(
          "Encounterd an error while trying to send final status to resource manager",
          ex);
    }
    
    return rc.getCode();
  }
}