
package com.efuture.titan.mysql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.packet.MySQLPacket;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.mysql.tasks.InitDbTask;
import com.efuture.titan.mysql.tasks.QueryTask;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.net.handler.NIOHandler;
import com.efuture.titan.security.Authenticator;
import com.efuture.titan.session.SessionState;

public class MySQLDriver extends NIOHandler {
  private static final Log LOG = LogFactory.getLog(MySQLDriver.class);
  private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

  private boolean isAuthenticated = false;

  public MySQLDriver(TitanConf conf) {
    super(conf);
    //this.sessionState = new MySQLSessionState(conf);
  }

  public void handle(NIOConnection connection, byte[] data) {
    MySQLFrontendConnection conn = (MySQLFrontendConnection) connection;
    MySQLSessionState ss;
    if (! conn.isClosed()) {
      ss = MySQLSessionState.get(conn);
    } else {
      // connection has closed
    }
    checkAuthentication(conn, data);
    processCommand(conn, data);
  }

  private void checkAuthentication(MySQLFrontendConnection conn, byte[] data) {
    MySQLSessionState ss = MySQLSessionState.get(conn);
    if (! ss.getIsAuthenticated()) {
      authenticate(ss, data);
      if (! ss.getIsAuthenticated()) {
        // error
        // conn.error();
        conn.close();
      } else {
        // ok
        conn.write(AUTH_OK);
      }
      return;
    }
  }

  private void authenticate(MySQLSessionState ss, byte[] data) {
    Authenticator auth = ss.getAuthenticator();
    if (auth == null) {
      ss.setIsAuthenticated(true);
    }
    // TODO
  }

  private void processCommand(MySQLFrontendConnection conn, byte[] data) {
    MySQLSessionState ss = MySQLSessionState.get(conn);
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
        LOG.info("Quit command");
        conn.close();
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
        conn.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        //source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        break;
    }
  }

}
