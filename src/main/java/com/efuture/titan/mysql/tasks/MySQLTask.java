
package com.efuture.titan.mysql.tasks;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public abstract class MySQLTask {

  protected MySQLFrontendConnection conn;

  public MySQLTask(MySQLFrontendConnection conn) {
    this.conn = conn;
  }

  abstract public void run();

}
