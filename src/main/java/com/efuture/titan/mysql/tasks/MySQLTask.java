
package com.efuture.titan.mysql.tasks;

public abstract class MySQLTask {

  protected MySQLFrontendConnection conn;

  public MySQLTask(MySQLFrontendConnection conn) {
    this.conn = conn;
  }

  abstract public void run();

}
