/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.knittingboar.sgd.iterativereduce;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.iterativereduce.ComputableMaster;
import com.cloudera.iterativereduce.ComputableWorker;
import com.cloudera.iterativereduce.Utils;
import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.iterativereduce.yarn.appmaster.ApplicationMasterService;
import com.cloudera.iterativereduce.yarn.appworker.ApplicationWorkerService;
import com.cloudera.iterativereduce.yarn.avro.generated.FileSplit;
import com.cloudera.iterativereduce.yarn.avro.generated.StartupConfiguration;
import com.cloudera.iterativereduce.yarn.avro.generated.WorkerId;
import com.cloudera.knittingboar.messages.iterativereduce.ParameterVectorGradientUpdatable;
import com.cloudera.knittingboar.utils.TestingUtils;
import com.google.common.io.Files;
//import com.cloudera.knittingboar.yarn.AvroUtils;


public class TestPOLRIterativeReduce {

  private InetSocketAddress masterAddress;
  private ExecutorService pool;
  private ApplicationMasterService<ParameterVectorGradientUpdatable> masterService;
  private Future<Integer> master;
  private ComputableMaster<ParameterVectorGradientUpdatable> computableMaster;
  private ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable>> workerServices;
  private ArrayList<Future<Integer>> workers;
  private ArrayList<ComputableWorker<ParameterVectorGradientUpdatable>> computableWorkers;
  private File baseDir;
  private Configuration configuration;
  
  
  @Before
  public void setUp() throws Exception {
    masterAddress = new InetSocketAddress(9999);
    pool = Executors.newFixedThreadPool(4);
    baseDir = Files.createTempDir();
    configuration =  new Configuration();
    configuration.setInt( "com.cloudera.knittingboar.setup.FeatureVectorSize", 10000 );
    configuration.setInt( "com.cloudera.knittingboar.setup.numCategories", 20);    
    configuration.setInt("com.cloudera.knittingboar.setup.BatchSize", 200);    
    configuration.setInt("com.cloudera.knittingboar.setup.NumberPasses", 1);    
    // local input split path
    configuration.set( "com.cloudera.knittingboar.setup.LocalInputSplitPath", "hdfs://127.0.0.1/input/0" );
    // setup 20newsgroups
    configuration.set( "com.cloudera.knittingboar.setup.RecordFactoryClassname", "com.cloudera.knittingboar.records.TwentyNewsgroupsRecordFactory");
    workerServices = new ArrayList<ApplicationWorkerService<ParameterVectorGradientUpdatable>>();
    workers = new ArrayList<Future<Integer>>();
    computableWorkers = new ArrayList<ComputableWorker<ParameterVectorGradientUpdatable>>();

    setUpMaster();

    setUpWorker("worker1");
    setUpWorker("worker2");
    setUpWorker("worker3");
  }
  
  @After
  public void teardown() throws Exception {
   FileUtils.deleteDirectory(baseDir); 
  }

  /**
   * TODO: give the system multiple files and create the right number of splits
   * 
   * TODO: StartupConfiguration needs to be fed from the Configuration object somehow
   * 
   * TODO: what event do I have for when all the work is done?
   * - maybe a "completion()" method in ComputableMaster ?
   * 
   * @throws Exception
   */
  public void setUpMaster() throws Exception {
    
    System.out.println( "start-ms:" + System.currentTimeMillis() );
    
    File inputFile = new File(baseDir, "kboar-shard-0.txt");
    TestingUtils.copyDecompressed("kboar-shard-0.txt.gz", inputFile);
    FileSplit split = FileSplit.newBuilder()
    .setPath(inputFile.getAbsolutePath()).setOffset(0).setLength(8348890)
    .build();    
    
    // hey MK, how do I set multiple splits or splits of multiple files?
    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(200).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);
    workers.put(Utils.createWorkerId("worker2"), conf);
    workers.put(Utils.createWorkerId("worker3"), conf);

    computableMaster = new POLRMasterNode();
    masterService = new ApplicationMasterService<ParameterVectorGradientUpdatable>(masterAddress,
        workers, computableMaster, ParameterVectorGradientUpdatable.class, null, configuration );

    master = pool.submit(masterService);
  }

  private void setUpWorker(String name) {
    //HDFSLineParser parser = new HDFSLineParser();
    
    TextRecordParser<ParameterVectorGradientUpdatable> parser = new TextRecordParser<ParameterVectorGradientUpdatable>();
    
    ComputableWorker<ParameterVectorGradientUpdatable> computableWorker = new POLRWorkerNode();
    final ApplicationWorkerService<ParameterVectorGradientUpdatable> workerService = new ApplicationWorkerService<ParameterVectorGradientUpdatable>(
        name, masterAddress, parser, computableWorker, ParameterVectorGradientUpdatable.class, configuration);

    Future<Integer> worker = pool.submit(new Callable<Integer>() {
      public Integer call() {
        return workerService.run();
      }
    });

    computableWorkers.add(computableWorker);
    workerServices.add(workerService);
    workers.add(worker);
  }

  @Test
  public void testWorkerService() throws Exception {
    // TODO tests without assertions are not tests
    workers.get(0).get();
    workers.get(1).get();
    workers.get(2).get();
    master.get();
/*
    // Bozo numbers
    assertEquals(Integer.valueOf(12100), computableWorkers.get(0).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(1).getResults().get());
    assertEquals(Integer.valueOf(12100), computableWorkers.get(2).getResults().get());
    assertEquals(Integer.valueOf(51570), computableMaster.getResults().get());
*/
   }
}