
package com.efuture.titan.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.common.util.StringUtils;

public class NIOConnector extends Thread {
  public static Log LOG = LogFactory.getLog(NIOConnector.class);
  private static ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

  private String name;
  private Selector selector;
  private BlockingQueue<BackendConnection> connectQueue;
  private NIOProcessor[] processors;
  private int nextProcessor;
  private long connectCount;

  public NIOConnector(String name) throws IOException {
    super.setName(name);
    this.name = name;
    this.selector = Selector.open();
    this.connectQueue = new LinkedBlockingQueue<BackendConnection>();
  }

  public long getConnectCount() {
    return connectCount;
  }

  public void setProcessors(NIOProcessor[] processors) {
    this.processors = processors;
  }

  public void postConnect(BackendConnection c) {
    connectQueue.offer(c);
    selector.wakeup();
  }

  @Override
  public void run() {
    Selector selector = this.selector;
    while (true) {
      ++connectCount;
      try {
        selector.select(1000L);
        connect(selector);
        Set<SelectionKey> keys = selector.selectedKeys();
        try {
          for (SelectionKey key : keys) {
            Object att = key.attachment();
            if (att != null && key.isValid() && key.isConnectable()) {
              finishConnect(key, att);
            } else {
              key.cancel();
            }
          }
        } finally {
          keys.clear();
        }
      } catch (Throwable e) {
        LOG.warn("Error in NIOConnector(" + name + "). " +
            StringUtils.stringifyException(e));
      }
    }
  }

  private void connect(Selector selector) {
    BackendConnection c = null;
    while ((c = connectQueue.poll()) != null) {
      try {
        c.connect(selector);
      } catch (Throwable e) {
        c.error(ErrorCode.ERR_CONNECT_SOCKET, e);
      }
    }
  }

  private void finishConnect(SelectionKey key, Object att) {
    BackendConnection c = (BackendConnection) att;
    try {
      if (c.finishConnect()) {
        clearSelectionKey(key);
        c.setId(ID_GENERATOR.getId());
        NIOProcessor processor = nextProcessor();
        c.setProcessor(processor);
        processor.postRegister(c);
      }
    } catch (Throwable e) {
      clearSelectionKey(key);
      c.error(ErrorCode.ERR_FINISH_CONNECT, e);
    }
  }

  private void clearSelectionKey(SelectionKey key) {
    if (key.isValid()) {
      key.attach(null);
      key.cancel();
    }
  }

  private NIOProcessor nextProcessor() {
    if (++nextProcessor == processors.length) {
      nextProcessor = 0;
    }
    return processors[nextProcessor];
  }

  /**
   * @brif backend ID Generator
   */
  private static class ConnectIdGenerator {
    private long connectId = 0L;
    private Object lock = new Object();

    private long getId() {
      synchronized (lock) {
        if (connectId >= Long.MAX_VALUE) {
          connectId = 0L;
        }
        return ++connectId;
      }
    }
  }

}
