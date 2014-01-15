
package com.efuture.titan.server;

import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.FrontendConnectionFactory;

public class ServerConnectionFactory extends FrontendConnectionFactory {
  private static final Log LOG = LogFactory.getLog(ServerConnectionFactory.class);

  public ServerConnectionFactory(TitanConf conf) {
    super(conf);
  }

  @Override
  public FrontendConnection getConnection(SocketChannel channel) {
    FrontendConnection conn = new ServerConnection(channel, handlers);
    return conn;
  }

}
