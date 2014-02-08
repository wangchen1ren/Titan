
package com.efuture.titan.mysql.net;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.CommandProcessor;
import com.efuture.titan.metadata.Meta;
import com.efuture.titan.metastore.NoSuchObjectException;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.AuthPacket;
import com.efuture.titan.mysql.net.packet.OkPacket;
import com.efuture.titan.mysql.net.packet.MySQLPacket;
import com.efuture.titan.mysql.processor.CommandProcessorFactory;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.net.NIOHandler;
import com.efuture.titan.security.AuthenticationException;
import com.efuture.titan.security.Authenticator;
import com.efuture.titan.security.AuthorizationException;
import com.efuture.titan.security.Authorizer;
import com.efuture.titan.session.SessionState;

public class MySQLNIOHandler extends NIOHandler {
  private static final Log LOG = LogFactory.getLog(MySQLNIOHandler.class);

  private boolean isAuthenticated = false;

  public MySQLNIOHandler(TitanConf conf) {
    super(conf);
    //this.sessionState = new MySQLSessionState(conf);
  }

  public void handle(NIOConnection connection, byte[] data) {
    MySQLFrontendConnection conn = (MySQLFrontendConnection) connection;
    if (conn.isClosed()) {
      return;
    }
    MySQLSessionState ss = MySQLSessionState.get(conn);
    if (! ss.isAuthenticated) {
      authenticate(ss, data);
    } else {
      processCommand(ss, data);
    }
  }


  /*==================================*/
  /*     Connection Authentication    */
  /*==================================*/

  private void authenticate(MySQLSessionState ss, byte[] data) {
    Authenticator authen = ss.getAuthenticator();
    if (authen == null) {
      LOG.warn("No authenticator, pass authenticate");
      ss.isAuthenticated = true;
    }
    AuthPacket auth = new AuthPacket();
    auth.read(data);
    try {
      authen.authenticate(auth.user, auth.password);
      if (auth.database != null && checkSetDb(ss, auth.database)) {
        ss.db = auth.database;
        ss.getFrontendConnection().write(AuthPacket.AUTH_OK);
      }
    } catch (AuthenticationException e) {
      String msg = "Access denied for user '" + auth.user + "'";
      LOG.warn("Authentication Failed: " + msg);
      ss.getFrontendConnection().writeErrMessage(
          (byte) 2, 
          ErrorCode.ER_ACCESS_DENIED_ERROR,
          msg);
    }
  }

  /*==================================*/
  /*         Process Commands         */
  /*==================================*/

  private void processCommand(MySQLSessionState ss, byte[] data) {
    switch (data[4]) {
      case MySQLPacket.COM_INIT_DB:
        LOG.info("[Command] INIT_DB");
        initDb(ss, data);
        break;
      case MySQLPacket.COM_QUERY:
        LOG.info("[Command] QUERY");
        query(ss, data);
        break;
      case MySQLPacket.COM_PING:
        LOG.info("[Command] PING");
        ss.getFrontendConnection().write(OkPacket.OK);
        break;
      case MySQLPacket.COM_QUIT:
        LOG.info("[Command] QUIT");
        ss.getFrontendConnection().close();
        break;
      case MySQLPacket.COM_PROCESS_KILL:
        LOG.info("[Command] PROCESS_KILL");
        ss.getFrontendConnection().writeErrMessage(
            ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        break;
      case MySQLPacket.COM_STMT_PREPARE:
        LOG.info("[Command] STATEMENT_PREPARE");
        ss.getFrontendConnection().writeErrMessage(
            ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        break;
      case MySQLPacket.COM_STMT_EXECUTE:
        LOG.info("[Command] STATEMENT_EXECUTE");
        ss.getFrontendConnection().writeErrMessage(
            ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        break;
      case MySQLPacket.COM_STMT_CLOSE:
        LOG.info("[Command] STATEMENT_CLOSE");
        ss.getFrontendConnection().writeErrMessage(
            ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        break;
      case MySQLPacket.COM_HEARTBEAT:
        LOG.info("[Command] HEARTBEAT");
        ss.getFrontendConnection().write(OkPacket.OK);
        break;
      default:
        LOG.info("[Command] UNKNOWN");
        ss.getFrontendConnection().writeErrMessage(
            ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        break;
    }
  }

  /*==================================*/
  /*             Commands             */
  /*==================================*/

  private void initDb(MySQLSessionState ss, byte[] data) {
    FrontendConnection conn = ss.getFrontendConnection();

    MySQLMessage mm = new MySQLMessage(data);
    mm.position(5);
    String db = mm.readString();

    // 检查db是否已经设置
    if (ss.db != null) {
      if (db.equals(ss.db)) {
        conn.write(OkPacket.OK);
      } else {
        conn.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR,
            "Not allowed to change the database!");
      }
      return;
    }
    if (checkSetDb(ss, db)) {
      ss.db = db;
      conn.write(OkPacket.OK);
    }
  }

  private boolean checkSetDb(MySQLSessionState ss, String db) {
    FrontendConnection conn = ss.getFrontendConnection();

    try {
      // check db exists
      Meta meta = Meta.get(ss.getConf());
      meta.getDatabase(db);
    } catch (Exception e) {
      conn.writeErrMessage(
          ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
      return false;
    }
    String user = ss.getAuthenticator().getUser();
    try {
      // check user has privilege
      Authorizer authorizer = ss.getAuthorizer();
      if (authorizer != null) {
        authorizer.authorize(user, db);
      }
    } catch (AuthorizationException e) {
      String s = "Access denied for user '" + user +
          "' to database '" + db + "'";
      conn.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
      return false;
    }
    return true;
  }


  private void query(MySQLSessionState ss, byte[] data) {
    FrontendConnection conn = ss.getFrontendConnection();
    // 取得语句
    MySQLMessage mm = new MySQLMessage(data);
    mm.position(5);
    String sql = null;
    try {
      sql = mm.readString(ss.charset);
      sql = sql.trim().toLowerCase();
    } catch (UnsupportedEncodingException e) {
      conn.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
          "Unknown charset '" + ss.charset + "'");
      return;
    }
    if (sql == null || sql.length() == 0) {
      conn.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
      return;
    }
    //sql = StringUtil.replace(sql, "`", "");
    // remove last ';'
    if (sql.endsWith(";")) {
      sql = sql.substring(0, sql.length() - 1); 
    }

    // 执行查询
    CommandProcessor processor = CommandProcessorFactory.get(ss, sql);
    processor.run(sql);
  }

}
