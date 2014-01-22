
package com.efuture.titan.mysql.processor;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public class KillProcessor implements CommandProcessor {

  @Override
  public void init() {
  }

  @Override
  public void run(String sql, MySQLFrontendConnection conn) {
    //TODO
    // process kill query
  }
}
