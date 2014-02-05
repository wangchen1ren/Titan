
package com.efuture.titan.net;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.net.buffer.BufferPool;
import com.efuture.titan.net.buffer.BufferQueue;
import com.efuture.titan.util.StringUtils;
import com.efuture.titan.util.TimeUtil;

public abstract class AbstractConnection implements NIOConnection {
  public static final Log LOG = LogFactory.getLog(AbstractConnection.class);

  private static final int DEFAULT_WRITE_QUEUE_CAPACITY = 16;

  protected TitanConf conf;

  protected final SocketChannel channel;
  protected NIOProcessor processor;
  protected NIOHandler handler;

  protected SelectionKey processKey;
  protected boolean isRegistered = false;
  protected boolean isSocketClosed;
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final ReentrantLock keyLock = new ReentrantLock();
  protected final ReentrantLock writeLock = new ReentrantLock();

  protected BufferQueue writeQueue;

  protected long startupTime;
  protected long lastReadTime;
  protected long lastWriteTime;

  public AbstractConnection(TitanConf conf, SocketChannel channel) {
    this.conf = conf;
    this.channel = channel;
    init();
  }

  public TitanConf getConf() {
    return conf;
  }

  public void setHandler(NIOHandler handler) {
    this.handler = handler;
  }

  private void init() {
    this.writeQueue = new BufferQueue(DEFAULT_WRITE_QUEUE_CAPACITY);

    this.startupTime = TimeUtil.currentTimeMillis();
    this.lastReadTime = startupTime;
    this.lastWriteTime = startupTime;
  }

  public boolean isClosed() {
    return isClosed.get();
  }

  /*==================================*/
  /*            Register              */
  /*==================================*/

  @Override
  public void register(NIOProcessor processor, Selector selector) throws IOException {
    //LOG.debug("Register");
    isRegistered = true;
    this.processor = processor;
    this.processKey = channel.register(selector, SelectionKey.OP_READ, this);
    //LOG.debug("Key: " + processKey);
  }

  /*==================================*/
  /*              Read                */
  /*==================================*/

  abstract public void read() throws IOException;

  /*==================================*/
  /*            Handle                */
  /*==================================*/

  @Override
  public void handle(byte[] data) {
    try {
      if (handler != null) {
        handler.handle(this, data);
      }
    } catch (Throwable e) {
      if (e instanceof ConnectionException) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        close();
        error(ErrorCode.ERR_CONNECT_SOCKET, e);
      } else {
        error(ErrorCode.ERR_HANDLE_DATA, e);
      }
    }
  }

  /*==================================*/
  /*             Write                */
  /*==================================*/

  @Override
  public void write(byte[] data) {
    ByteBuffer buffer;
    int offset = 0;
    int len = data.length;
    while (true) {
      buffer = BufferPool.getInstance().allocate();
      int remaining = buffer.remaining();
      if (remaining >= len) {
        buffer.put(data, offset, len);
        write(buffer);
        break;
      } else {
        buffer.put(data, offset, remaining);
        write(buffer);
        offset += remaining;
        len -= remaining;
      }
    }
  }

  private final void write(ByteBuffer buffer) {
    if (isClosed.get()) {
      LOG.warn("Connection is closed.");
      BufferPool.getInstance().recycle(buffer);
      return;
    }
    if (!isRegistered) {
      BufferPool.getInstance().recycle(buffer);
      LOG.warn("Connection not registed.");
      close();
      return;
    }
    if (buffer.position() == 0) {
      LOG.debug("empty buffer write to connection !!!");
      return;
    }

    try {
      writeQueue.offer(buffer);
      int writeQueueStatus = writeQueue.getStatus();
      switch (writeQueueStatus) {
        case BufferQueue.NEARLY_EMPTY: 
          this.writeQueueAvailable();
          break;
        case BufferQueue.NEARLY_FULL: 
          this.writeQueueBlocked();
          break;
      }
    } catch (InterruptedException e) {
      error(ErrorCode.ERR_PUT_WRITE_QUEUE, e);
      return;
    }
    processor.processWrite(this);
  }

  public void writeQueueAvailable() {
  }

  public void writeQueueBlocked() {
  }

  /*==================================*/
  /*           Write Out              */
  /*==================================*/

  public void writeByQueue() throws IOException {
    if (isClosed.get()) {
      return;
    }
    final ReentrantLock lock = this.writeLock;
    lock.lock();
    try {
      // 满足以下两个条件时，切换到基于事件的写操作。
      // 1.当前key对写事件不该兴趣。
      // 2.write0()返回false。
      if ((processKey.interestOps() & SelectionKey.OP_WRITE) == 0) {
        write0();
      }
    } finally {
      lock.unlock();
    }
  }

  public void writeByEvent() {
  }

  private void write0() throws IOException {
    ByteBuffer buffer;
    int written = 0;
    // 写出发送队列中的数据块
    if ((buffer = writeQueue.poll()) != null) {
      buffer.flip();
      while (buffer.hasRemaining()) {
        written = channel.write(buffer);
        if (written > 0) {
          lastWriteTime = TimeUtil.currentTimeMillis();
        }
      }
      BufferPool.getInstance().recycle(buffer);
    }
  }

  /*==================================*/
  /*     Close and Clear functions    */
  /*==================================*/

  /**
   * 清理遗留资源
   */
  protected void cleanup() {
    // 回收发送缓存
    ByteBuffer buffer;
    while ((buffer = writeQueue.poll()) != null) {
      BufferPool.getInstance().recycle(buffer);
    }
  }

  @Override
  public void close() {
    LOG.info("Close connection.");
    if (!isClosed.get()) {
      isClosed.set(true);
      cleanup();
      closeSocket();
    }
  }

  private synchronized void clearSelectionKey() {
    if (processKey != null && processKey.isValid()) {
      processKey.attach(null);
      processKey.cancel();
    }
  }

  private boolean closeSocket() {
    clearSelectionKey();
    SocketChannel channel = this.channel;
    if (channel != null) {
      boolean isSocketClosed = true;
      Socket socket = channel.socket();
      if (socket != null) {
        try {
          socket.close();
        } catch (Throwable e) {
          LOG.warn("Close socket error: " + StringUtils.stringifyException(e));
        }
        isSocketClosed = socket.isClosed();
      }
      try {
        channel.close();
      } catch (Throwable e) {
      }
      return isSocketClosed && (!channel.isOpen());
    } else {
      return true;
    }
  }

  public void error(int errCode, Throwable t) {
    LOG.error("Error in connection with error code: " + errCode + " " +
        StringUtils.stringifyException(t));
  }

}
