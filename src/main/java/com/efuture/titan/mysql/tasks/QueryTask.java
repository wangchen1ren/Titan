
package com.efuture.titan.mysql.tasks;

public class QueryTask extends MySQLTask {

  public QueryTask(MySQLFrontendConnection conn, byte[] data) {
    super(conn);
    this.data = data;
  }

  public void run() {
  }
}
