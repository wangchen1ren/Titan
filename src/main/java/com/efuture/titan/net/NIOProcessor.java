package com.efuture.titan.net;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import org.opencloudb.buffer.BufferPool;
//import org.opencloudb.statistic.CommandCount;
import com.efuture.titan.common.util.NameableExecutor;

public final class NIOProcessor {
  public static final Log LOG = LogFactory.getLog(NIOProcessor.class);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 64;
  private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4096;

  private TitanConf conf;

  private String name;
  private NIOReactor reactor;

  // BufferPool that used to exchange data between frontend and backend connections
  private BufferPool bufferPool;
  //private NameableExecutor executor;
  private ConcurrentMap<Long, FrontendConnection> frontends;
  private ConcurrentMap<Long, BackendConnection> backends;
  //private CommandCount commands;
  //private long netInBytes;
  //private long netOutBytes;

  public NIOProcessor(TitanConf conf, String name) throws IOException {
    this(conf, name, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE,
        conf.getIntVar(ConfVars.TITAN_SERVER_EXECUTORS));
  }

  public NIOProcessor(TitanConf conf, String name, int nExecutor) throws IOException {
    this(conf, name, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE, nExecutor);
  }

  public NIOProcessor(TitanConf conf, String name, int bufferSize,
      int chunkSize, int nExecutor) throws IOException {
    this.conf = conf;
    this.name = name;
    this.reactor = new NIOReactor(name);
    this.bufferPool = new BufferPool(bufferSize, chunkSize);
    //if (nExecutor > 0) {
    //  this.executor = NameableExecutor.newExecutor(name + "-E", nExecutor);
    //}
    this.frontends = new ConcurrentHashMap<Long, FrontendConnection>();
    this.backends = new ConcurrentHashMap<Long, BackendConnection>();
    //this.commands = new CommandCount();
  }

  public String getName() {
    return this.name;
  }

  public NIOReactor getReactor() {
    return this.reactor;
  }

  public BufferPool getBufferPool() {
    return bufferPool;
  }

  //public int getRegisterQueueSize() {
  //  return reactor.getRegisterQueue().size();
  //}

  //public int getWriteQueueSize() {
  //  return reactor.getWriteQueue().size();
  //}

  //public NameableExecutor getExecutor() {
  //  return executor;
  //}

  public void startup() {
    reactor.startup();
  }

  //public void postRegister(NIOConnection c) {
  //  reactor.postRegister(c);
  //}

  //public void postWrite(NIOConnection c) {
  //  reactor.postWrite(c);
  //}

  /*
  public CommandCount getCommands() {
    return commands;
  }
  */

  //public long getNetInBytes() {
  //  return netInBytes;
  //}

  //public void addNetInBytes(long bytes) {
  //  netInBytes += bytes;
  //}

  //public long getNetOutBytes() {
  //  return netOutBytes;
  //}

  //public void addNetOutBytes(long bytes) {
  //  netOutBytes += bytes;
  //}

  //public long getReactCount() {
  //  return reactor.getReactCount();
  //}

  public void addFrontend(FrontendConnection c) {
    frontends.put(c.getId(), c);
  }

  public ConcurrentMap<Long, FrontendConnection> getFrontends() {
    return frontends;
  }

  public void addBackend(BackendConnection c) {
    backends.put(c.getId(), c);
  }

  public ConcurrentMap<Long, BackendConnection> getBackends() {
    return backends;
  }

  /**
   * 定时执行该方法，回收部分资源。
   */
  public void check() {
    checkFrontend();
    checkBackend();
  }

  private void checkFrontend() {
    Iterator<Entry<Long, FrontendConnection>> it = frontends.entrySet()
        .iterator();
    while (it.hasNext()) {
      FrontendConnection c = it.next().getValue();

      if (c == null) {
        it.remove();
        continue;
      }

      if (c.isClosed()) {
        it.remove();
        c.cleanup();
      } else {
        c.checkIdle();
      }
    }
  }

  private void checkBackend() {
    Iterator<Entry<Long, BackendConnection>> it = backends.entrySet()
        .iterator();
    while (it.hasNext()) {
      BackendConnection c = it.next().getValue();

      if (c == null) {
        it.remove();
        continue;
      }

      if (c.isClosed()) {
        it.remove();
        c.cleanup();
      } else {
        c.checkIdle();
      }
    }
  }

}
