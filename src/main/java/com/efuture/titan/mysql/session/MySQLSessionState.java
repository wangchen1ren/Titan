
package com.efuture.titan.mysql.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.exec.QueryExecutor;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.security.Authenticator;
import com.efuture.titan.session.SessionState;

public class MySQLSessionState extends SessionState {

  private QueryExecutor queryExecutor;

  public static MySQLSessionState get(MySQLFrontendConnection conn) {
    MySQLSessionState ss = (MySQLSessionState) SessionState.get(conn);
    if (ss == null) {
      ss = new MySQLSessionState(conn.getConf());
      put(conn, ss);
    }
    return ss;
  }

  public MySQLSessionState(TitanConf conf) {
    super(conf);
  }

  public Authenticator getAuthenticator() {
    return null;
  }

  public QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }

}
