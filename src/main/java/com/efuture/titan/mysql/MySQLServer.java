
package com.efuture.titan.mysql;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.mysql.net.MySQLFrontendConnectionFactory;
import com.efuture.titan.mysql.net.MySQLNIOHandler;
import com.efuture.titan.net.NIOProcessorManager;
import com.efuture.titan.net.NIOServer;
import com.efuture.titan.util.StringUtils;

public class MySQLServer {
  private static final Log LOG = LogFactory.getLog(MySQLServer.class);
  private static final String SERVER_NAME = "TitanServer";

  private TitanConf conf;
  private int port;
  private NIOServer server;
  private MySQLFrontendConnectionFactory factory;
  private NIOProcessorManager processorMgr;

  // use to close processorMgr
  private boolean isOwnProcessorMgr;

  public MySQLServer(TitanConf conf) throws Exception {
    this(conf, new NIOProcessorManager(conf, SERVER_NAME), true);
  }

  public MySQLServer(TitanConf conf,
      NIOProcessorManager processorMgr) throws Exception {
    this(conf, processorMgr, false);
  }

  public MySQLServer(TitanConf conf,
      NIOProcessorManager processorMgr,
      boolean isOwnProcessorMgr) throws Exception {

    this.conf = conf;
    this.processorMgr = processorMgr;
    this.isOwnProcessorMgr = isOwnProcessorMgr;
    this.factory = new MySQLFrontendConnectionFactory(conf);
    this.factory.setNIOHandler(new MySQLNIOHandler(conf));
    this.port = conf.getIntVar(ConfVars.TITAN_SERVER_PORT);
    this.server = new NIOServer(SERVER_NAME, port,
        factory, processorMgr);
  }

  public void start() {
    try {
      server.start();
    } catch (Exception e) {
      LOG.error("Failed to start MySQL server. " +
          StringUtils.stringifyException(e));
    }
  }

  public void shutdown() {
    server.shutdown();
    if (isOwnProcessorMgr) {
      processorMgr.stop();
    }
  }

}
