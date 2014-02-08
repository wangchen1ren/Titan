
package com.efuture.titan.mysql;

import junit.framework.TestCase;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.util.StringUtils;

public class DriverTest extends TestCase {

  private TitanConf conf;
  private MySQLFrontendConnection conn;
  private Driver driver;

  public void setUp() {
    try {
      conf = new TitanConf();
      driver = new Driver();
      conn = new MySQLFrontendConnection(conf, null);
      MySQLSessionState ss = MySQLSessionState.get(conn);
      driver.init(ss);
    } catch (Exception e) {
      fail("Error in setup: " + StringUtils.stringifyException(e));
    }
  }

  public void testCompile() {
    try {
      String sql = "select /*balance*/1 from t as t1;";
      driver.compile(sql);
    } catch (Exception e) {
      fail("Error in test: " + StringUtils.stringifyException(e));
    }
  }

}
