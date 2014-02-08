
package com.efuture.titan.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.QueryPlan;

public class BlockingSession implements Session {
  private static final Log LOG = LogFactory.getLog(BlockingSession.class);

  private TitanConf conf;
  private SessionState ss;

  public BlockingSession(TitanConf conf, SessionState ss) {
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

    /*
    RouteResultset rrs = plan.getRouteResultSet();
    // 检查路由结果是否为空
    RouteResultsetNode[] nodes = rrs.getNodes();
    if (nodes == null || nodes.length == 0) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_NO_DB_ERROR, "No dataNode selected");
      return;
    }

    // 选择执行方式
    if (nodes.length == 1) {
      singleNodeExecutor.execute(nodes[0], this, rrs.getFlag());
    } else {
      // 多数据节点，非事务模式下，执行的是可修改数据的SQL，则后端为事务模式。
      boolean autocommit = source.isAutocommit();
      if (autocommit && isModifySQL(type)) {
        autocommit = false;
      }
      multiNodeExecutor.execute(nodes, autocommit, this, rrs.getFlag());
    }
    */
  }

  @Override
  public void commit() {
    if (ss.txInterrupted) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_YES, "Transaction error, need to rollback.");
      return;
    }
  }

  @Override
  public void rollback() {
    if (ss.txInterrupted) {
      ss.txInterrupted = true;
    }
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
