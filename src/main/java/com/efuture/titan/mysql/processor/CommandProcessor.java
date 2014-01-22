
package com.efuture.titan.mysql.processor;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public interface CommandProcessor {

  public void init();

  public void run(String sql, MySQLFrontendConnection conn);

}
