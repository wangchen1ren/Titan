
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

public class NIOReactor {
  public static final Log LOG = LogFactory.getLog(NIOReactor.class);

  private String name;

  private ReadReactor readReactor;
  private Thread readThread;

  private WriteReacotr writeReactor;
  private Thread writeThread;

  public NIOReactor(String name) throws IOException {
    this.name = name;
    this.readReactor = new ReadReactor();
    this.readThread = new Thread(readReactor, name + "-R");
    this.WriteReactor = new WriteReactor();
    this.writeThread = new Thread(WriteReactor, name + "-W");
  }

  public void startup() {
    this.readThread.start();
    this.writeThread.start();
  }

  public void postRegister(NIOConnection c) {
    readReactor.registerQueue.offer(c);
    readReactor.selector.wakeup();
  }

  public BlockingQueue<NIOConnection> getRegisterQueue() {
    return readReactor.registerQueue;
  }

  public long getReactCount() {
    return readReactor.reactCount;
  }

  public void postWrite(NIOConnection c) {
    WriteReactor.writeQueue.offer(c);
  }

  public BlockingQueue<NIOConnection> getWriteQueue() {
    return WriteReactor.writeQueue;
  }

  private class ReadReactor implements Runnable {
    private Selector selector;
    private BlockingQueue<NIOConnection> registerQueue;
    private long reactCount;

    private ReadReactor() throws IOException {
      this.selector = Selector.open();
      this.registerQueue = new LinkedBlockingQueue<NIOConnection>();
    }

    @Override
    public void run() {
      Selector selector = this.selector;
      while (true) {
        ++reactCount;
        try {
          // Suppressed tempory read keys should not clear

          selector.select(1000L);
          register(selector);
          Set<SelectionKey> keys = selector.selectedKeys();
          // Set<SelectionKey> keys = selectedKeys;

          try {
            for (SelectionKey key : keys) {
              Object att = key.attachment();
              if (att != null &&
                  key.isValid() &&
                  (key.readyOps() & SelectionKey.OP_READ) != 0) {
                read((NIOConnection) att);
              } else {
                key.cancel();
              }
            }
          } finally {
            keys.clear();
          }
        } catch (Exception e) {
          LOG.warn("Error in NIOReactor(" + name + "). " +
              StringUtils.stringifyException(e));
        }
      }
    }

    private void register(Selector selector) {
      NIOConnection c = null;
      while ((c = registerQueue.poll()) != null) {
        try {
          c.register(selector);
        } catch (Throwable e) {
          c.error(ErrorCode.ERR_REGISTER, e);
        }
      }
    }

    private void read(NIOConnection c) {
      try {
        c.read();
      } catch (Throwable e) {
        c.error(ErrorCode.ERR_READ, e);
        c.close("exception:" + e.toString());
      }
    }

  }

  private class WriteReactor implements Runnable {
    private BlockingQueue<NIOConnection> writeQueue;

    private WriteReactor() {
      this.writeQueue = new LinkedBlockingQueue<NIOConnection>();
    }

    @Override
    public void run() {
      NIOConnection c = null;
      while (true) {
        try {
          if ((c = writeQueue.take()) != null) {
            c.writeByQueue();
          }
        } catch (Exception e) {
          LOG.warn("Error in NIOReactor(" + name + "). " +
              StringUtils.stringifyException(e));
          c.close("exception:" + e.toString());
        }
      }
    }

  }

}
