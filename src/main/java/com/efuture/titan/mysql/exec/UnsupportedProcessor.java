
package com.efuture.titan.mysql.exec;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public class UnsupportedProcessor implements Processor {

  @Override
  public void process(String sql, MySQLFrontendConnection conn) {
    conn.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
        "Unsupported statement");
  }
}
