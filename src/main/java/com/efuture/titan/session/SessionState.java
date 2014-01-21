
package com.efuture.titan.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.security.Authenticator;

public class SessionState {

  private static Map<NIOConnection, SessionState> sessionMap =
      new ConcurrentHashMap<NIOConnection, SessionState>();

  protected TitanConf conf;

  protected boolean isAuthenticated = false;

  public String db;
  public String charset;

  public String user;
  public String host;

  public long lastInsertId;

  public static SessionState get(NIOConnection conn) {
    SessionState ss = sessionMap.get(conn);
    /*
    if (ss == null) {
      ss = new SessionState(conn.getConf());
      put(conn, ss);
    }
    */
    return ss;
  }

  public static void put(NIOConnection conn, SessionState ss) {
    sessionMap.put(conn, ss);
  }

  public static void remove(NIOConnection conn) {
    sessionMap.remove(conn);
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
