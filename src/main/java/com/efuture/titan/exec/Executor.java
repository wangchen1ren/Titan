
package com.efuture.titan.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.QueryPlan;
import com.efuture.titan.exec.Executor;

public class Executor implements Executor {
  private static final Log LOG = LogFactory.getLog(Executor.class);

  private TitanConf conf;
  private SessionState ss;

  private NodeWorker worker;

  public Executor(TitanConf conf, SessionState ss) {
    this.conf = conf;
    this.ss = ss;
  }

  @Override
  public SessionState getSessionState() {
    return ss;
  }

  @Override
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

    if (tasks.size() == 1) {
      worker = new SingleNodeWorker(conf, ss);
    } else {
      worker = new SingleNodeWorker(conf, ss);
      //worker = new MultiNodeWorker(conf, ss);
    }
    worker.execute(plan);
  }

  @Override
  public void commit() {
    if (ss.txInterrupted) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_YES, "Transaction error, need to rollback.");
      return;
    }
    worker.commit();
  }

  @Override
  public void rollback() {
    if (ss.txInterrupted) {
      ss.txInterrupted = true;
    }
    worker.rollback();
  }

  @Override
  public void cancel() {
  }

  @Override
  public void terminate() {
  }

  @Override
  public void close() {
  }

}
