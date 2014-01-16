
package com.efuture.titan.mysql.tasks;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public class PingTask extends MySQLTask {

  public PingTask(MySQLFrontendConnection conn) {
    super(conn);
  }

  public void run() {
  }

}
