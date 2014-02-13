
package com.efuture.titan.mysql.net;

import java.net.InetAddress;

import junit.framework.TestCase;
import org.apache.derby.drda.NetworkServerControl;

import com.efuture.titan.metastore.DataSource;

public class MySQLBackendConnectionTest extends TestCase {

  private NetworkServerControl server;
  private DataSource dataSource;

  public void setUp() {
    try {
      DataSource dataSource = new DataSource(
      server = new NetworkServerControl(
          InetAddress.getByName("localhost"), 1527);
      server.start(null);
    } catch (Exception e) {
      fail("Error in setup: " + StringUtils.stringifyException(e));
    }
  }

  public void tearDown() {
  }
}
