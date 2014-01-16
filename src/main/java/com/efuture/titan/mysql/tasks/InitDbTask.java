
package com.efuture.titan.mysql.tasks;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.MySQLMessage;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.packet.OkPacket;
import com.efuture.titan.mysql.session.MySQLSessionState;

public class InitDbTask extends MySQLTask {

  private byte[] data;

  public InitDbTask(MySQLFrontendConnection conn, byte[] data) {
    super(conn);
    this.data = data;
  }

  public void run() {
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
}
