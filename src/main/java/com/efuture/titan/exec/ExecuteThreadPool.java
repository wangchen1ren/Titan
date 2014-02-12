
package com.efuture.titan.exec;

import com.efuture.titan.util.NameableExecutor;

public class ExecuteThreadPool extends NameableExecutor {

  public static ExecuteThreadPool threadPool;

  public static ExecuteThreadPool get(TitanConf conf) {
    if (threadPool == null) {
      int poolSize = 0;
      threadPool = NameableExecutor.newExecutor("Backend Executor", poolSize);
    }
    return threadPool;
  }

}
