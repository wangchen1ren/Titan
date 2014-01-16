
package com.efuture.titan.mysql.net;

import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.FrontendConnectionFactory;

public class MySQLFrontendConnectionFactory extends FrontendConnectionFactory {
  public static final Log LOG = LogFactory.getLog(MySQLFrontendConnectionFactory.class);

  public MySQLFrontendConnectionFactory(TitanConf conf) {
    super(conf);
  }

  @Override
  public FrontendConnection getConnection(SocketChannel channel) {
    FrontendConnection conn = new MySQLFrontendConnection(conf, channel);
    conn.setHandler(handler);
    return conn;
  }
}
