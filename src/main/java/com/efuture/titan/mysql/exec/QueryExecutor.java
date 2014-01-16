
package com.efuture.titan.mysql.exec;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.packet.OkPacket;
import com.efuture.titan.mysql.parse.MySQLParse;
import com.efuture.titan.mysql.session.MySQLSessionState;

public class QueryExecutor {

  private TitanConf conf;
  private MySQLFrontendConnection conn;

  public QueryExecutor(TitanConf conf, MySQLFrontendConnection conn) {
    this.conf = conf;
    this.conn = conn;
  }

  public void query(String sql) {
    int rs = MySQLParse.parse(sql);
    switch (rs & 0xff) {
      case MySQLParse.EXPLAIN:
        //ExplainHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.SET:
        //SetHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.SHOW:
        //ShowHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.SELECT:
        //SelectHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.START:
        //StartHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.BEGIN:
        //BeginHandler.handle(sql, conn); 
        break;
      case MySQLParse.SAVEPOINT:
        //SavepointHandler.handle(sql, conn); 
        break;
      case MySQLParse.KILL:
        //KillHandler.handle(sql, rs >>> 8, conn); 
        break;
      case MySQLParse.KILL_QUERY:
        conn.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
            "Unsupported command");
        break;
      case MySQLParse.USE:
        //UseHandler.handle(sql, conn, rs >>> 8); 
        break;
      case MySQLParse.COMMIT:
        //conn.commit();
        break;
      case MySQLParse.ROLLBACK:
        //conn.rollback();
        break;
      case MySQLParse.HELP:
        conn.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
        break;
      case MySQLParse.MYSQL_CMD_COMMENT:
        conn.write(OkPacket.OK);
        break;
      case MySQLParse.MYSQL_COMMENT:
        conn.write(OkPacket.OK);
        break;
      default:
        execute(sql, rs & 0xff);
    }
  }

  private void execute(String sql, int type) {
    MySQLSessionState ss = MySQLSessionState.get(conn);
    /*
    // 状态检查
    if (ss.txInterrupted) {
      conn.writeErrMessage(ErrorCode.ER_YES,
          "Transaction error, need to rollback.");
      return;
    }   

    // 检查当前使用的DB
    String db = ss.db;
    if (db == null) {
      conn.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
      return;
    }
    SchemaConfig schema = MycatServer.getInstance().getConfig()
        .getSchemas().get(db);
    if (schema == null) {
      writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
          + db + "'");
      return;
    }   

    // 路由计算
    RouteResultset rrs = null;
    try {
      rrs = ServerRouter.route(schema, type, sql, ss.charset, conn);
    } catch (SQLNonTransientException e) {
      StringBuilder s = new StringBuilder();
      LOG.warn(s.append(this).append(sql).toString(), e); 
      String msg = e.getMessage();
      conn.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e 
          .getClass().getSimpleName() : msg);
      return;
    }

    // session执行
    //session.execute(rrs, type);
    */
  }

}
