
package com.efuture.titan.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.security.Authenticator;

public class SessionState {

  private static Map<NIOConnection, SessionState> sessionMap =
      new ConcurrentHashMap<NIOConnection, SessionState>();

  private TitanConf conf;

  private boolean isAuthenticated = false;

  public static SessionState get(NIOConnection conn) {
    SessionState ss = sessionMap.get(conn);
    if (ss == null) {
      ss = new SessionState(conn.getConf());
      sessionMap.put(conn, ss);
    }
    return ss;
  }

  public SessionState(TitanConf conf) {
    this.conf = conf;
  }

  public Authenticator getAuthenticator() {
    return null;
  }

  public boolean getIsAuthenticated() {
    return isAuthenticated;
  }

  public void setIsAuthenticated(boolean auth) {
    this.isAuthenticated = auth;
  }

}
