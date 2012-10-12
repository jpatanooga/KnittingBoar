package com.cloudera.knittingboar.yarn.appworker;

import java.net.InetSocketAddress;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.CompoundAdditionWorker;
import com.cloudera.knittingboar.yarn.UpdateableInt;
import com.cloudera.knittingboar.yarn.appworker.RecordParser;

public class TestApplicationWorker {

  @Test
  public void testApplicationWorker() throws Exception {
    RecordParser<UpdateableInt> parser = new HDFSLineParser<UpdateableInt>(UpdateableInt.class);
    CompoundAdditionWorker worker = new CompoundAdditionWorker();
    ApplicationWorker<UpdateableInt> aw = new ApplicationWorker<UpdateableInt>(parser, worker, UpdateableInt.class);
    String[] args = { "--master-addr", "localhost:9999", "--worker-id", "worker-1234" };
    
    aw.run(args);
  }
}
