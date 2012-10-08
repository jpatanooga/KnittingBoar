package com.cloudera.knittingboar.yarn;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.knittingboar.yarn.appworker.ComputableWorker;

public class CompoundAdditionWorker implements ComputableWorker<UpdateableInt, String> {
  private static final Log LOG = LogFactory.getLog(CompoundAdditionWorker.class);
  
  int masterTotal = 0;
  UpdateableInt workerTotal;
  
  @Override
  public UpdateableInt compute(List<String> records) {
    int total = 0;
    
    for(String s : records) {
      Integer i = Integer.parseInt(s);
      total += i;
    }
    
    //masterTotal = total / 10;
    
    //if (workerTotal == null)
      workerTotal = new UpdateableInt();
    
    workerTotal.set(masterTotal + total);
    LOG.debug("Current total=" + workerTotal.get() 
        + ", records=" + records.toString());
    
    return workerTotal;
  }
  
  public UpdateableInt getResults() {
    return workerTotal;
  }

  @Override
  public void update(UpdateableInt t) {
    masterTotal = t.get();
  }

  @Override
  public void setup(Configuration c) {
    // TODO Auto-generated method stub
    
  }
}

