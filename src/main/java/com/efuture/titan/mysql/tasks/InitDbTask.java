
package com.efuture.titan.mysql.tasks;

public class InitDbTask extends MySQLTask {

  public InitDbTask(MySQLFrontendConnection conn, byte[] data) {
    super(conn);
    this.data = data;
  }

  public void run() {
    SessionState ss = SessionState.get(conn);

    MySQLMessage msg = new MySQLMessage(data);
    mm.position(5);
    String db = mm.readString();

    // 检查db是否已经设置
    if (ss.db != null) {
      if (db.equals(ss.db)) {
        write(writeToBuffer(OkPacket.OK, allocate()));
      } else {
        writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR,
            "Not allowed to change the database!");
      }   
      return;
    }   

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
  }
}
