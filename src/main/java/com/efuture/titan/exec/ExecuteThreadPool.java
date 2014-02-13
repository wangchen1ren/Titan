
package com.efuture.titan.exec;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.util.NameableExecutor;

public class ExecuteThreadPool {

  public static ExecuteThreadPool threadPool;

  public static ExecuteThreadPool get(TitanConf conf) {
    if (threadPool == null) {
      threadPool = new ExecuteThreadPool(conf);
    }
    return threadPool;
  }

  private int poolSize;
  private NameableExecutor executor;

  public ExecuteThreadPool(TitanConf conf) {
    this.poolSize = conf.getIntVar(ConfVars.TITAN_SERVER_EXECUTE_THREAD_POOL_SIZE);
    this.executor = NameableExecutor.newExecutor("Backend Executor", poolSize);
  }

  public void execute(Runnable runnable) {
    this.executor.execute(runnable);
  }

}
