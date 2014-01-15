
package com.efuture.titan.server;

import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.NIOHandler;

public class ServerConnection extends FrontendConnection {
  private static final Log LOG = LogFactory.getLog(ServerConnection.class);

  public ServerConnection(SocketChannel channel,
      List<NIOHandler> handlers) {
    super(channel, handlers);
  }

}
