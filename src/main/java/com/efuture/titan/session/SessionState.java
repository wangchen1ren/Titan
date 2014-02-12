
package com.efuture.titan.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.security.Authenticator;
import com.efuture.titan.security.Authorizer;

public class SessionState {

  public static final int MYSQL = 1;

  private static Map<FrontendConnection, SessionState> sessionMap =
      new ConcurrentHashMap<FrontendConnection, SessionState>();

  protected TitanConf conf;
  protected int type;
  protected FrontendConnection feConn;

  public String db;
  public String charset;
  public boolean isAuthenticated = false;

  public String user;
  public String host;

  public long lastInsertId;

  public boolean txInterrupted;
  public boolean txIsolation; 

  private Authenticator authenticator = null;
  private Authorizer authorizer = null;

  private Executor executor;

  public static SessionState get(FrontendConnection conn) {
    SessionState ss = sessionMap.get(conn);
    /*
    if (ss == null) {
      ss = new SessionState(conn.getConf());
      put(conn, ss);
    }
    */
    return ss;
  }

  public static void put(FrontendConnection conn, SessionState ss) {
    sessionMap.put(conn, ss);
  }

  public static void remove(FrontendConnection conn) {
    sessionMap.remove(conn);
  }

  public SessionState(TitanConf conf,
      FrontendConnection conn) {
    this.conf = conf;
    this.feConn = conn;
  }

  public FrontendConnection getFrontendConnection() {
    return feConn;
  }

  public TitanConf getConf() {
    return conf;
  }

  public Authenticator getAuthenticator() {
    return authenticator;
  }

  public Authorizer getAuthorizer() {
    return authorizer;
  }

  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  public Executor getExecutor() {
    return executor;
  }

  public void close() {
    // close session
    session.close();
    // remove from map
    SessionState.remove(feConn);
  }

}
