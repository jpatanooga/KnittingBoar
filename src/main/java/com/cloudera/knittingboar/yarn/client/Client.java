package com.cloudera.wovenwabbit.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {
  private static final Log LOG = LogFactory.getLog(Client.class);

  private Configuration conf;
  private YarnConfiguration yarnConf;
  private YarnRPC rpc;
  private ClientRMProtocol applicationsManager;
  private InetSocketAddress rmAddress;
  
  private String appName = "Hello World";
  
  public Client() throws Exception {
		conf = new Configuration();
		yarnConf = new YarnConfiguration(conf);
		rpc = YarnRPC.create(conf);
		rmAddress = yarnConf.getSocketAddr(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_PORT);
		
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		applicationsManager = ((ClientRMProtocol)rpc.getProxy(
				ClientRMProtocol.class, rmAddress, conf));
		
		getClusterMetrics();
		getClusterNodes();
		
		GetNewApplicationResponse newApp = getApplication();
		ApplicationId appId = newApp.getApplicationId();
		
		int minMem = newApp.getMinimumResourceCapability().getMemory();
		int maxMem = newApp.getMaximumResourceCapability().getMemory();
		
		LOG.info("Min memory=" + minMem);
		LOG.info("Max memory=" + maxMem);
		
		LOG.info("Setting up application submission context");
		
		ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
		appContext.setApplicationId(appId);
		appContext.setApplicationName(appName);
		
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path("/tmp/nothing");
		Path dst = new Path(fs.getUri() + "/tmp/nothing");
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus dstStatus = fs.getFileStatus(dst);
		LocalResource nLocalResource = Records.newRecord(LocalResource.class);
		nLocalResource.setType(LocalResourceType.FILE);
		nLocalResource.setVisibility(LocalResourceVisibility.APPLICATION);
		nLocalResource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
		nLocalResource.setTimestamp(dstStatus.getModificationTime());
		nLocalResource.setSize(dstStatus.getLen());
		localResources.put("nothingness", nLocalResource);
		
		amContainer.setLocalResources(localResources);
		
		String command = "/tmp/nothing";
		List<String> commands = new ArrayList<String>();
		commands.add(command);
		amContainer.setCommands(commands);
		
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(minMem);
		amContainer.setResource(capability);
		
		appContext.setAMContainerSpec(amContainer);
				
		SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
		appRequest.setApplicationSubmissionContext(appContext);
		
		LOG.info("Submitting application to ASM");
		applicationsManager.submitApplication(appRequest);
		
		monitorApplication(appId);
		
	}
  
  private void monitorApplication(ApplicationId appId) throws YarnRemoteException {
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.debug("Thread interrupted");
      }
      
      GetApplicationReportRequest reportReq = Records.newRecord(GetApplicationReportRequest.class);
      reportReq.setApplicationId(appId);
      
      GetApplicationReportResponse reportRes = applicationsManager.getApplicationReport(reportReq);
      ApplicationReport report = reportRes.getApplicationReport();
      
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
      
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus status = report.getFinalApplicationStatus();
      
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == status) {
          LOG.info("App completed. Exiting loop");
          return;
        } else {
          LOG.info("App didn't return succesfully. Breaking loop");
          return;
        }
      } else if(YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application killed. Breaking loop");
        return;
      }
      
      if(false) { // client timeout or something
        LOG.info("Timeout reached");
        //killApp(appId);
        return;
      }
    }
  }
  
  private GetNewApplicationResponse getApplication() throws YarnRemoteException {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = applicationsManager.getNewApplication(request);

    LOG.info("Got new application id=" + response.getApplicationId());
    
    return response;
  }

  private void getClusterMetrics() throws IOException {
    GetClusterMetricsRequest clusterMetricsReq = Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsRes = applicationsManager.getClusterMetrics(clusterMetricsReq);

    LOG.info("Got Cluster metric info from ASM"
        + ", numNodeManagers=" + clusterMetricsRes.getClusterMetrics().getNumNodeManagers());
  }
  
  private void getClusterNodes() throws IOException {
    GetClusterNodesRequest clusterNodesReq = Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesRes = applicationsManager.getClusterNodes(clusterNodesReq);

    LOG.info("Got Cluster nodes info from ASM");
    
    for (NodeReport node : clusterNodesRes.getNodeReports()) {
      LOG.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId()
          + ", nodeAddress=" + node.getHttpAddress()
          + ", nodeRackName=" + node.getRackName()
          + ", nodeNumContainers=" + node.getNumContainers()
          + ", nodeHealthStatus=" + node.getNodeHealthStatus());
    }
  }

  public static void main(String[] args) throws Exception {
    new Client();
  }
  
  
}