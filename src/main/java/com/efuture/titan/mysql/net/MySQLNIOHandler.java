
package com.efuture.titan.mysql.net;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.OkPacket;
import com.efuture.titan.mysql.net.packet.MySQLPacket;
import com.efuture.titan.mysql.processor.CommandProcessor;
import com.efuture.titan.mysql.processor.CommandProcessorFactory;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.net.NIOConnection;
import com.efuture.titan.net.NIOHandler;
import com.efuture.titan.security.Authenticator;
import com.efuture.titan.session.SessionState;

public class MySQLNIOHandler extends NIOHandler {
  private static final Log LOG = LogFactory.getLog(MySQLNIOHandler.class);
  private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

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
    Authenticator auth = ss.getAuthenticator();
    if (auth == null) {
      ss.isAuthenticated = true;
    }
    ss.getFrontendConnection().write(AUTH_OK);
  }

  /*==================================*/
  /*         Process Commands         */
  /*==================================*/

  private void processCommand(MySQLSessionState ss, byte[] data) {
    switch (data[4]) {
      case MySQLPacket.COM_INIT_DB:
        //initDb(conn, data);
        break;
      case MySQLPacket.COM_QUERY:
        query(ss, data);
        break;
      case MySQLPacket.COM_PING:
        //ping(conn);
        break;
      case MySQLPacket.COM_QUIT:
        //quit(conn);
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
        MySQLFrontendConnection conn = (MySQLFrontendConnection) ss.getFrontendConnection();
        conn.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        //source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        break;
    }
  }

  /*==================================*/
  /*             Commands             */
  /*==================================*/

  private void initDb(MySQLFrontendConnection conn, byte[] data) {
    MySQLSessionState ss = MySQLSessionState.get(conn);

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

    // temporary
    // TODO
    ss.db = db;
    conn.write(OkPacket.OK);

    /*
    // 检查schema的有效性
    if (db == null || !privileges.schemaExists(db)) {
      writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
          + db + "'");
      return;
    }   
    if (!privileges.userExists(user, host)) {
      writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR,
          "Access denied for user '" + user + "'");
      return;
    }   
    Set<String> schemas = privileges.getUserSchemas(user);
    if (schemas == null || schemas.size() == 0 || schemas.contains(db)) {
      this.schema = db; 
      write(writeToBuffer(OkPacket.OK, allocate()));
    } else {
      String s = "Access denied for user '" + user + "' to database '"
          + db + "'";
      writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
    }
    */
  }


  private void query(MySQLSessionState ss, byte[] data) {
    MySQLFrontendConnection conn = (MySQLFrontendConnection) ss.getFrontendConnection();
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

  private void ping(MySQLFrontendConnection conn) {
    //TODO
  }

  private void quit(MySQLFrontendConnection conn) {
    LOG.info("Quit Command.");
    conn.close();
  }

}
