package com.cloudera.knittingboar.yarn.appmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.UpdateableInt;

public class TestApplicationMaster {

  private static Log LOG = LogFactory.getLog(TestApplicationMaster.class);
  
  @Test
  public void testApplicationMaster() throws Exception {
    CompoundAdditionMaster cam = new CompoundAdditionMaster();
    ApplicationMaster<UpdateableInt> am = new ApplicationMaster<UpdateableInt>(cam, UpdateableInt.class);
    Configuration conf = new Configuration();
    ToolRunner.run(conf, am, new String[] {});
  }
  
  public static void main(String[] args) throws Exception {
    CompoundAdditionMaster cam = new CompoundAdditionMaster();
    ApplicationMaster<UpdateableInt> am = new ApplicationMaster<UpdateableInt>(cam, UpdateableInt.class);

    ToolRunner.run(am, args);
  }
}
