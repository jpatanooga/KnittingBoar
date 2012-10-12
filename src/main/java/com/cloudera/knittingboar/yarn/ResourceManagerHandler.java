package com.cloudera.knittingboar.yarn;

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

public class ResourceManagerHandler {

  private static final Log LOG = LogFactory.getLog(ResourceManagerHandler.class);
  
  private Configuration conf;
  private ContainerId masterContainerId;

  private AMRMProtocol amResourceManager;
  private ClientRMProtocol clientResourceManager;
  private AtomicInteger rmRequestId = new AtomicInteger();
  
  public ResourceManagerHandler(Configuration conf, ContainerId masterContainerId) {
    this.conf = conf;
    this.masterContainerId = masterContainerId;
  }
  
  public AMRMProtocol getAMResourceManager() {
    if (amResourceManager != null)
      return amResourceManager;
    
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    YarnRPC rpc = YarnRPC.create(yarnConf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));

    LOG.info("Connecting to the resource manager (scheduling) at " + rmAddress);
    amResourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
        rmAddress, yarnConf);
    
    return amResourceManager;
  }
  
  public ClientRMProtocol getClientResourceManager() {
    if (clientResourceManager != null)
      return clientResourceManager;
    
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    YarnRPC rpc = YarnRPC.create(yarnConf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS));
    
    LOG.info("Connecting to the resource manager (client) at " + rmAddress);
    
    clientResourceManager = (ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, yarnConf);
    
    return clientResourceManager;
  }
  
  public ApplicationId getApplicationId() throws YarnRemoteException  {
    if (clientResourceManager == null)
      throw new IllegalStateException(
          "Cannot get an application ID befire connecting to resource manager!");
    
    GetNewApplicationRequest appReq = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse appRes = clientResourceManager.getNewApplication(appReq);
    LOG.info("Got a new application with id=" + appRes.getApplicationId());
    
    return appRes.getApplicationId();
  }
  
  public void submitApplication(ApplicationId appId, String appName,
      Map<String, LocalResource> localResources, 
      List<String> commands, int memory) throws YarnRemoteException,
      URISyntaxException {
    
    if (clientResourceManager == null)
      throw new IllegalStateException(
          "Cannot submit an application without connecting to resource manager!");

    ApplicationSubmissionContext appCtx = Records.newRecord(ApplicationSubmissionContext.class);
    appCtx.setApplicationId(appId);
    appCtx.setApplicationName(appName);
    
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);

    ContainerLaunchContext containerCtx = Records.newRecord(ContainerLaunchContext.class);
    containerCtx.setLocalResources(localResources);
    containerCtx.setCommands(commands);
    containerCtx.setResource(capability);
    appCtx.setAMContainerSpec(containerCtx);

    SubmitApplicationRequest submitReq = Records.newRecord(SubmitApplicationRequest.class);
    submitReq.setApplicationSubmissionContext(appCtx);
    
    LOG.info("Submitting application to ASM");
    clientResourceManager.submitApplication(submitReq);
    
    // Don't return anything, ASM#submit returns an empty response
  }
  
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnRemoteException {

    if (clientResourceManager == null)
      throw new IllegalStateException(
          "Cannot query for a report without first connecting!");

    GetApplicationReportRequest req = Records
        .newRecord(GetApplicationReportRequest.class);
    req.setApplicationId(appId);

    return clientResourceManager.getApplicationReport(req).getApplicationReport();
  }
  
  public RegisterApplicationMasterResponse registerApplicationMaster(String host, int port)
      throws YarnRemoteException {
    
    if (amResourceManager == null)
      throw new IllegalStateException(
          "Cannot register application master before connecting to the resource manager!");
    
    LOG.info("Sending application registration request");

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    
    request.setApplicationAttemptId(masterContainerId.getApplicationAttemptId());
    request.setHost(host);
    request.setRpcPort(port);
    request.setTrackingUrl("");
    
    return amResourceManager.registerApplicationMaster(request);
  }
  
  public AMResponse allocateRequest(
      List<ResourceRequest> requestedContainers,
      List<ContainerId> releasedContainers) throws YarnRemoteException {
    
    if (amResourceManager == null)
      throw new IllegalStateException(
          "Cannot send allocation request before connecting to the resource manager!");

    LOG.info("Sending allocation request");
    
    AllocateRequest request = Records.newRecord(AllocateRequest.class);
    request.setResponseId(rmRequestId.incrementAndGet());
    request.setApplicationAttemptId(masterContainerId.getApplicationAttemptId());
    request.addAllAsks(requestedContainers);
    request.addAllReleases(releasedContainers);

    return amResourceManager.allocate(request).getAMResponse();
  }
  
  public void finishApplication(String diagnostics,
      FinalApplicationStatus finishState) throws YarnRemoteException {
    
    if (amResourceManager == null)
      throw new IllegalStateException(
          "Cannot finish an application without connecting to resource manager!");

    FinishApplicationMasterRequest request = Records.newRecord(FinishApplicationMasterRequest.class);
    request.setAppAttemptId(masterContainerId.getApplicationAttemptId());
    request.setDiagnostics(diagnostics);
    request.setFinishApplicationStatus(finishState);

    LOG.info("Sending finish application notification "
        + ", state=" + finishState
        + ", diagnostics=" + diagnostics);
    
    amResourceManager.finishApplicationMaster(request);
  }
}
