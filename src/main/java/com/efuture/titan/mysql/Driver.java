
package com.efuture.titan.mysql;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.processor.CommandProcessor;
//import com.efuture.titan.route.RoutePlan;

public class Driver implements CommandProcessor {

  private MySQLFrontendConnection conn;

  //private RoutePlan plan;

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
    //stmt = removeSchema(stmt, schema.getName());
    //RouteResultset rrs = new RouteResultset(stmt, sqlType);

    //plan = new RoutePlan(sql);
    return 0;
  }

  public int execute() {
    return 0;
  }

}
