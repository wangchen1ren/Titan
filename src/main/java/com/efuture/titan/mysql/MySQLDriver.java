
package com.efuture.titan.mysql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.net.handler.NIOHandler;
import com.efuture.titan.session.SessionState;

public class MySQLDriver extends NIOHandler {
  private static final Log LOG = LogFactory.getLog(MySQLDriver.class);
  private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

  private SessionState sessionState;

  private boolean isAuthenticated = false;

  public MySQLDriver(TitanConf conf) {
    super(conf);
    //this.sessionState = new SessionState(conf);
  }

  public void handle(NIOConnection connection, byte[] data) {
    MySQLFrontentConnection conn = (MySQLFrontendConnection) connection;
    SessionState ss;
    if (! conn.isClosed()) {
      ss = SessionState.get(conn);
    } else {
      // connection has closed
    }
    checkAuthentication(conn, data);
    processCommand(conn, data);
  }

  private void checkAuthentication(MySQLFrontendConnection conn, byte[] data) {
    SessionState ss = SessionState.get(conn);
    if (! ss.getIsAuthenticated()) {
      authenticate(ss, data);
      if (! ss.getIsAuthenticated()) {
        // error
        // conn.error();
        closeConnection(conn);
      }
      return;
    }
  }

  private void authenticate(SessionState ss, byte[] data) {
    Authenticator auth = ss.getAuthenticator();
    if (auth == null) {
      ss.setIsAuthenticated(true);
      conn.write(AUTH_OK);
    }
  }

  private void processCommand(MySQLFrontendConnection conn, byte[] data) {
    SessionState ss = SessionState.get(conn);
    switch (data[4]) {
      case MySQLPacket.COM_INIT_DB:
        //source.initDB(data);
        InitDbTask initDbTask = new InitDbTask(conn, data);
        initDbTask.run();
        break;
      case MySQLPacket.COM_QUERY:
        QueryTask queryTask = new QueryTask(conn, data);
        queryTask.run();
        //source.query(data);
        break;
      case MySQLPacket.COM_PING:
        //source.ping();
        break;
      case MySQLPacket.COM_QUIT:
        //source.close("do quit");
        break;
      case MySQLPacket.COM_PROCESS_KILL:
        //source.kill(data);
        break;
      case MySQLPacket.COM_STMT_PREPARE:
        //source.stmtPrepare(data);
        break;
      case MySQLPacket.COM_STMT_EXECUTE:
        //source.stmtExecute(data);
        break;
      case MySQLPacket.COM_STMT_CLOSE:
        //source.stmtClose(data);
        break;
      case MySQLPacket.COM_HEARTBEAT:
        //source.heartbeat(data);
        break;
      default:
        //source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        break;
    }
  }

  private void closeConnection(MySQLFrontendConnection conn) {
    SessionState.remove(conn);
    conn.close();
  }
}
