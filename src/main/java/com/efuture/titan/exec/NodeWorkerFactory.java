
package com.efuture.titan.exec;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.exec.MySQLSingleNodeWorker;
import com.efuture.titan.session.SessionState;

public class NodeWorkerFactory {

  public static NodeWorker make(TitanConf conf,
      SessionState ss, int nTasks) {
    return new MySQLSingleNodeWorker(conf, ss);
  }
}
