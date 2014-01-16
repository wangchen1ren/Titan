
package com.efuture.titan.mysql.net;

import java.io.IOException;

import com.efuture.titan.net.NIOProcessorManager;
import com.efuture.titan.net.NIOServer;

public class MySQLServer extends NIOServer {

  public MySQLServer(String name, int port,
      MySQLFrontendConnectionFactory factory,
      NIOProcessorManager processorMgr) throws IOException {
    super(name, port, factory, processorMgr);
  }
}
