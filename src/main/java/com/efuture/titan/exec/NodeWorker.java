
package com.efuture.titan.exec;

import com.efuture.titan.exec.QueryPlan;

public interface NodeWorker {

  public void execute(QueryPlan plan);

  public void commit();

  public void rollback();

  public void terminate() throws InterruptedException;
}
