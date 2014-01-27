
package com.efuture.titan.mysql.processor;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.session.MySQLSessionState;

public interface CommandProcessor {

  public void init(MySQLSessionState ss);

  public void run(String sql);

}
