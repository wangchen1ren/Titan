
package com.efuture.titan.mysql.tasks;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.MySQLMessage;
import com.efuture.titan.mysql.exec.QueryExecutor;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.session.MySQLSessionState;

public class QueryTask extends MySQLTask {

  private byte[] data;

  public QueryTask(MySQLFrontendConnection conn, byte[] data) {
    super(conn);
    this.data = data;
  }

  public void run() {
    MySQLSessionState ss = MySQLSessionState.get(conn);
    QueryExecutor executor = ss.getQueryExecutor();
    if (executor != null) {
      // 取得语句
      MySQLMessage mm = new MySQLMessage(data);
      mm.position(5);
      String sql = null;
      try {
        sql = mm.readString(ss.charset);
      //} catch (UnsupportedEncodingException e) {
      } catch (Exception e) {
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
      executor.query(sql);
    } else {
      conn.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
          "Query unsupported!");
    }   
  }
}
