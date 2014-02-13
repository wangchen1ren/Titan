
package com.efuture.titan.exec;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.session.SessionState;

public class Executor {
  private static final Log LOG = LogFactory.getLog(Executor.class);

  private TitanConf conf;
  private SessionState ss;

  private NodeWorker worker;

  public Executor(TitanConf conf, SessionState ss) {
    this.conf = conf;
    this.ss = ss;
  }

  public SessionState getSessionState() {
    return ss;
  }

  public void execute(QueryPlan plan) {
    if (ss.txInterrupted) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_YES, "Transaction error, need to rollback.");
      return;
    }

    List<DataNodeTask> tasks = plan.getDataNodeTasks();

    if (tasks == null || tasks.size() == 0) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_NO_DB_ERROR, "No dataNode selected");
      return;
    }

    worker = NodeWorkerFactory.make(conf, ss, tasks.size());
    worker.execute(plan);
  }

  public void commit() {
    if (ss.txInterrupted) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_YES, "Transaction error, need to rollback.");
      return;
    }
    worker.commit();
  }

  public void rollback() {
    if (ss.txInterrupted) {
      ss.txInterrupted = true;
    }
    worker.rollback();
  }

  public void cancel() {
  }

  public void terminate() {
  }

  public void close() {
  }

}
