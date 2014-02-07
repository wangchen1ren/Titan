
package com.efuture.titan.metastore;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import com.efuture.titan.common.LogUtil;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.util.StringUtils;

public class TitanMetaStore extends ThriftTitanMetaStore {
  private static final Log LOG = LogFactory.getLog(TitanMetaStore.class);

  public static final int MIN_WORKER_THREADS = 100;
  public static final int MAX_WORKER_THREADS = 10000;

  protected TitanConf conf;
  protected int port;
  protected TThreadPoolServer         server;
  protected TThreadPoolServer.Args    sargs;

  private int maxWorkerThreads;
  private int minWorkerThreads;
  private boolean tcpKeepAlive;

  protected AtomicBoolean stopped = new AtomicBoolean(true);

  public static ITMSHandler newTMSHandler(String name, TitanConf conf) {
    return new TMSHandler(name, conf);
  }

  public TitanMetaStore(TitanConf conf) {
    this.conf = conf;
    port = conf.getIntVar(ConfVars.TITAN_METASTORE_PORT);
    initServer();
  }

  private void initServer() {
    minWorkerThreads = conf.getIntVar(ConfVars.TITAN_METASTORE_MIN_WORKERS);
    if (minWorkerThreads < MIN_WORKER_THREADS) {
      minWorkerThreads = MIN_WORKER_THREADS;
    }
    maxWorkerThreads = conf.getIntVar(ConfVars.TITAN_METASTORE_MAX_WORKERS);
    if (maxWorkerThreads > MAX_WORKER_THREADS) {
      maxWorkerThreads = MAX_WORKER_THREADS;
    }
    //tcpKeepAlive = conf.getBoolVar(ConfVars.TITAN_METASTORE_TCP_KEEP_ALIVE);
    try {
      ITMSHandler handler = newTMSHandler("new metaserver", conf);
      TProcessor processor = new TSetIpAddressProcessor<ITMSHandler>(handler);

      LOG.info("Titan metastore is going to start on port " + port);
      //TServerTransport transport = tcpKeepAlive ? 
      //  new TServerSocketKeepAlive(port) : new TServerSocket(port);
      TServerTransport transport = new TServerSocket(port);
      sargs = new TThreadPoolServer.Args(transport)
        .processor(processor)
        .transportFactory(new TTransportFactory())
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(minWorkerThreads)
        .maxWorkerThreads(maxWorkerThreads);
      server = new TThreadPoolServer(sargs);
    } catch (Throwable e) {
      LOG.fatal("Unable to start metastore on port " + port, e);
      throw new RuntimeException(e);
    }
  }

  public int getPort() {
    return port;
  }

  public void start() {
    LOG.info("Starting titan metastore ...");
    stopped.set(false);
    server.serve();
  }

  public void stop() {
    LOG.info("Stopping titan metastore ...");
    stopped.set(true);
    server.stop();
  }

  public static void main(String[] args) {
    try {
      LogUtil.initLog4j("TitanMetaStore");
      TitanConf conf = new TitanConf();
      TitanMetaStore server = new TitanMetaStore(conf);
      server.start();
    } catch (Exception e) {
      LOG.fatal("Got exception TitanMetaStore: "
          + StringUtils.stringifyException(e));
      System.exit(1);
    } finally {
      LOG.info("TitanMetaStore stop.");
    }   
    System.exit(0);
  }   
}
