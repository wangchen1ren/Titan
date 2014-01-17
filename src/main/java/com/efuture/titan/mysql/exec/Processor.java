
package com.efuture.titan.mysql.exec;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public interface Processor {

  public void process(String sql, MySQLFrontendConnection conn);

}
