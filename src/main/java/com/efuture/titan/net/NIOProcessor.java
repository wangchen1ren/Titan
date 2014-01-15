package com.efuture.titan.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import com.efuture.titan.statistic.CommandCount;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.util.NameableExecutor;
import com.efuture.titan.util.StringUtils;

public final class NIOProcessor {
  public static final Log LOG = LogFactory.getLog(NIOProcessor.class);

  private TitanConf conf;
  private String name;

  private NameableExecutor executor;
  private List<NIOConnection> connectionList;

  private R r;
  private Thread rThread;

  private W w;
  private Thread wThread;

  public NIOProcessor(TitanConf conf, String name) throws IOException {
    this.conf = conf;
    this.name = name;

    int nExecutor = conf.getIntVar(ConfVars.TITAN_SERVER_EXECUTORS_PER_PROCESSOR);
    this.executor = NameableExecutor.newExecutor(name + "-E", nExecutor);

    connectionList = new ArrayList<NIOConnection>();

    this.r = new R();
    this.rThread = new Thread(r, name + "-R");
    this.w = new W();
    this.wThread = new Thread(w, name + "-W");
  }

  public void start() {
    this.rThread.start();
    this.wThread.start();
  }

  public void stop() {
    r.stop();
    this.rThread.interrupt();
    w.stop();
    this.wThread.interrupt();
  }

  public NameableExecutor getExecutor() {
    return executor;
  }

  public void register(NIOConnection conn) throws IOException {
    conn.register(this, this.r.selector);
    connectionList.add(conn);
    this.r.selector.wakeup();
  }

  public void processWrite(NIOConnection conn) {
    w.writeQueue.offer(conn);
  }

  private class R implements Runnable {
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private Selector selector;

    private R() throws IOException {
      this.selector = Selector.open();
    }

    public void stop() {
      stopped.set(true);
      try {
        this.selector.close();
      } catch (Exception e) {
        // ignore
      }
    }

    @Override
    public void run() {
      LOG.debug("Read thread started");
      while (! stopped.get()) {
        try {
          //selector.select(1000L);
          selector.select(1000L);
          Set<SelectionKey> keys = selector.selectedKeys();
          //LOG.debug("keys: " + keys);

          try {
            for (SelectionKey key : keys) {
              Object att = key.attachment();
              if (att != null && key.isValid() &&
                  (key.readyOps() & SelectionKey.OP_READ) != 0) {
                ((NIOConnection) att).read();
              } else {
                key.cancel();
              }
            }
          } finally {
            keys.clear();
          }
        } catch (Exception e) {
          LOG.warn("Error in read thread: " +
              StringUtils.stringifyException(e));
        }
      }
    }
  }

  private class W implements Runnable {
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private BlockingQueue<NIOConnection> writeQueue;

    private W() {
      this.writeQueue = new LinkedBlockingQueue<NIOConnection>();
    }

    public void stop() {
      stopped.set(true);
    }

    @Override
    public void run() {
      NIOConnection c = null;
      while (! stopped.get()) {
        try {
          if ((c = writeQueue.take()) != null) {
            c.writeByQueue();
          }
        } catch (Exception e) {
          LOG.warn("Error in process write(" + name + "). " +
              StringUtils.stringifyException(e));
          if (c != null) {
            c.close();
          }
        }
      }
    }

  }

}
