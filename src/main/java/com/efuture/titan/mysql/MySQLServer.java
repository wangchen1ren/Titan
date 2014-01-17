
package com.efuture.titan.mysql;

import java.io.IOException;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnectionFactory;
import com.efuture.titan.mysql.net.MySQLCommandDriver;
import com.efuture.titan.net.NIOProcessorManager;
import com.efuture.titan.net.NIOServer;

public class MySQLServer {

  private TitanConf conf;
  private NIOServer server;
  private MySQLFrontendConnectionFactory factory;
  private NIOProcessorManager processorMgr;

  public MySQLServer(TitanConf conf) {
    this.conf = conf;
  }

  public MySQLServer(TitanConf conf, NIOProcessorManager processorMgr) {
    this.conf = conf;
  }

  private void init() {
    factory = new MySQLFrontendConnectionFactory(conf);
    factory.setNIOHandler(new MySQLCommandDriver(conf));
  }

  public void start() {
    try {
      server.start();
    } catch (Exception e) {
    }
  }

  public void shutdown() {
    server.shutdown();
  }

}
