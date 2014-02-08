
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

  private static Map<FrontendConnection, SessionState> sessionMap =
      new ConcurrentHashMap<FrontendConnection, SessionState>();

  protected TitanConf conf;
  protected FrontendConnection feConn;

  public String db;
  public String charset;
  public boolean isAuthenticated = false;

  public String user;
  public String host;

  public long lastInsertId;

  private Authenticator authenticator = null;
  private Authorizer authorizer = null;

  private Session session;

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

  public Session getSession() {
    if (session == null) {
      String sessionMode = conf.getVar(ConfVars.TITAN_SERVER_SESSION_MODE).toUpperCase();
      if (sessionMode.equals("BLOCKING")) {
        session = new BlockingSession();
      } else if (sessionMode.equals("NONBLOCKING")) {
        session = new NonblockingSession();
      } else {
        // default blocking
        session = new BlockingSession();
      }
    }
    return session;
  }


}
