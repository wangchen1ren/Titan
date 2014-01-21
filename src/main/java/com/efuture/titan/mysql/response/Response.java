
package com.efuture.titan.mysql.response;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;

public abstract class Response {

  protected MySQLFrontendConnection conn;

  public void setFrontendConnection(MySQLFrontendConnection conn) {
    this.conn = conn;
  }

  abstract public void response();
}
