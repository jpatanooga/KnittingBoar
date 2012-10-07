package com.cloudera.wovenwabbit.yarn;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.knittingboar.yarn.appmaster.ComputableMaster;

public class CompoundAdditionMaster implements ComputableMaster<UpdateableInt> {
  private static final Log LOG = LogFactory.getLog(CompoundAdditionMaster.class);
  
  private UpdateableInt masterTotal;
  
  @Override
  public UpdateableInt compute(Collection<UpdateableInt> workerUpdates,
      Collection<UpdateableInt> masterUpdates) {

    int total = 0;
    
    for (UpdateableInt i : workerUpdates) {
      total += i.get();
    }
    
    for (UpdateableInt i : masterUpdates) {
      total += i.get();
    }
    
    //if (masterTotal == null)
      masterTotal = new UpdateableInt();
    
    masterTotal.set(total);
    LOG.debug("Current total=" + masterTotal.get() 
        + ", workerUpdates=" + toStrings(workerUpdates) 
        + ", masterUpdates=" + toStrings(masterUpdates));
    
    return masterTotal;
  }

  private String toStrings(Collection<UpdateableInt> c) {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    
    for (UpdateableInt i : c) {
      sb.append(i.get()).append(", ");
    }

    sb.append("]");
    return sb.toString();
    
  }
  @Override
  public UpdateableInt getResults() {
    return masterTotal;
  }
}
