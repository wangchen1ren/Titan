
package com.efuture.titan.mysql;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.processor.CommandProcessor;

public class Driver implements CommandProcessor {

  private MySQLFrontendConnection conn;

  public void init() {
  }

  public void run(String sql, MySQLFrontendConnection conn) {
    this.conn = conn;

    // pre-hook

    int ret = compile(sql);

    // lock
    ret = execute();

    // unlock
    
    // post-hook
  }

  public int compile(String sql) {
    return 0;
  }

  public int execute() {
    return 0;
  }

}
