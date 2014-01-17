
package com.efuture.titan.mysql.exec;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public class CommitProcessor implements Processor {

  @Override
  public void process(String sql, MySQLFrontendConnection conn) {
    QueryExecutor.commit(conn);
  }
}
