package com.cloudera.knittingboar.yarn.appmaster;

import org.junit.Test;

import com.cloudera.knittingboar.yarn.CompoundAdditionMaster;
import com.cloudera.knittingboar.yarn.UpdateableInt;

public class TestApplicationMaster {

  @Test
  public void testApplicationMaster() throws Exception {
    System.out.println(System.getProperty("user.dir"));
    CompoundAdditionMaster cam = new CompoundAdditionMaster();
    ApplicationMaster<UpdateableInt> am = new ApplicationMaster<UpdateableInt>(cam, UpdateableInt.class);
    am.run(new String[]{});
  }
}
