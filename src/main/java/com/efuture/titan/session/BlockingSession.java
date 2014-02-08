
package com.efuture.titan.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;

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
  public void execute() {
    if (ss.txInterrupted) {
      ss.getFrontendConnection().writeErrMessage(
          ErrorCode.ER_YES, "Transaction error, need to rollback.");
      return;
    }
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
