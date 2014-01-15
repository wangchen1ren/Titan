
package com.efuture.titan.net;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.net.buffer.BufferPool;
import com.efuture.titan.net.buffer.BufferQueue;
//import com.efuture.titan.server.ServerConnection;
import com.efuture.titan.util.StringUtils;
import com.efuture.titan.util.TimeUtil;

public class BaseConnection implements NIOConnection {
  public static final Log LOG = LogFactory.getLog(BaseConnection.class);

  private static final int DEFAULT_WRITE_QUEUE_CAPACITY = 16;
  private static final int MYSQL_PACKET_HEADER_SIZE = 4;
  private static final int MAX_PACKET_SIZE = 16 << 20;

  protected final SocketChannel channel;
  protected NIOProcessor processor;
  protected List<NIOHandler> handlers;

  protected SelectionKey processKey;
  protected boolean isRegistered = false;
  protected boolean isSocketClosed;
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final ReentrantLock keyLock = new ReentrantLock();
  protected final ReentrantLock writeLock = new ReentrantLock();

  protected ByteBuffer readBuffer;
  protected int readBufferOffset;
  protected BufferQueue writeQueue;

  protected long startupTime;
  protected long lastReadTime;
  protected long lastWriteTime;

  public BaseConnection(SocketChannel channel,
      List<NIOHandler> handlers) {
    this.channel = channel;
    this.handlers = handlers;
    init();
  }

  private void init() {
    isRegistered = false;
    this.readBuffer = BufferPool.getInstance().allocate();
    this.readBufferOffset = 0;
    this.writeQueue = new BufferQueue(DEFAULT_WRITE_QUEUE_CAPACITY);

    this.startupTime = TimeUtil.currentTimeMillis();
    this.lastReadTime = startupTime;
    this.lastWriteTime = startupTime;
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

  @Override
  public void read() throws IOException {
    //LOG.debug("READ");
    if (! isRegistered) {
      LOG.warn("Connection not registered!");
      close();
      return;
    }
    //dumpBuffer(readBuffer);
    int got = channel.read(readBuffer);
    lastReadTime = TimeUtil.currentTimeMillis();
    if (got < 0) {
      close();
      throw new EOFException("socket closed.");
    } else if (got == 0) {
      return;
    } else {
      //LOG.debug("Got bytes: " + got);
      //dumpBuffer(readBuffer);
      readFromBuffer();
    }
  }

  public void dumpBuffer(ByteBuffer buffer, int offset) {
    byte[] bufferArray = buffer.array();
    StringBuilder sb = new StringBuilder();
    for (int i = offset; i < readBuffer.position(); ++i) {
      /*
         if (bufferArray[i] >= 'a' && bufferArray[i] <= 'z' ||
         bufferArray[i] >= 'A' && bufferArray[i] <= 'Z' ||
         bufferArray[i] >= '0' && bufferArray[i] <= '9' 
         ) { 
         sb.append((char) bufferArray[i]);
         } else {
         sb.append((int) bufferArray[i]);
         }
         */
      sb.append((int) bufferArray[i]);
      sb.append(" ");
    }
    System.err.println("Buffer: " + sb.toString());
    LOG.debug("Buffer: " + sb.toString());
  }

  private void readFromBuffer() {
    //LOG.debug("Read from buffer");
    // 处理数据
    int packetLength = 0;
    int position = readBuffer.position();
    while (true) {
      //dumpBuffer(readBuffer, readBufferOffset);
      packetLength = getPacketLength(readBuffer, readBufferOffset);
      //LOG.debug("Packet Length: " + packetLength);

      // 未达到可计算数据包长度的数据
      // 未到达一个数据包的数据
      if (packetLength == -1 ||
          position < readBufferOffset + packetLength) {   
        if (! readBuffer.hasRemaining()) {
          expandReadBuffer();
        }
        break;
      }

      // 提取一个数据包的数据进行处理
      readBuffer.position(readBufferOffset);
      byte[] data = new byte[packetLength];
      readBuffer.get(data, 0, packetLength);
      //LOG.debug("Data: " + new String(data));
      //LOG.debug("Handle data: " + data);
      handle(data);
      // 设置偏移量
      readBufferOffset += packetLength;
      readBuffer.position(position);
      
      if (position == readBufferOffset) {// 数据正好全部处理完毕
        readBuffer.clear();
        readBufferOffset = 0;
        break;
      }
      // 还有剩余数据未处理
    }

  }

  /**
   * 获取数据包长度，默认是MySQL数据包，其他数据包重载此方法。
   */
  protected int getPacketLength(ByteBuffer buffer, int offset) {
    int res = 0;
    if (buffer.position() < offset + MYSQL_PACKET_HEADER_SIZE) {
      res = -1;
    } else {
      int length = buffer.get(offset) & 0xff;
      length |= (buffer.get(++offset) & 0xff) << 8;
      length |= (buffer.get(++offset) & 0xff) << 16;
      res = length + MYSQL_PACKET_HEADER_SIZE;
    }
    return res;
  }

  /**
   * 检查ReadBuffer容量，不够则扩展当前缓存，直到最大值。
   */
  private void expandReadBuffer() {
    // 当偏移量为0时需要扩容，否则移动数据至偏移量为0的位置。
    if (readBufferOffset == 0) {
      if (readBuffer.capacity() >= MAX_PACKET_SIZE) {
        throw new IllegalArgumentException("Packet size over the limit.");
      }
      int size = readBuffer.capacity() << 1;
      if (size > MAX_PACKET_SIZE) {
        size = MAX_PACKET_SIZE;
      }
      ByteBuffer tmp = readBuffer;
      ByteBuffer newBuffer = ByteBuffer.allocate(size);
      readBuffer.position(readBufferOffset);
      newBuffer.put(readBuffer);
      readBuffer = newBuffer;
      // 回收扩容前的缓存块
      BufferPool.getInstance().recycle(tmp);
    } else {
      readBuffer.position(readBufferOffset);
      readBuffer.compact();
      readBufferOffset = 0;
    }
  }

  /*==================================*/
  /*            Handle                */
  /*==================================*/

  @Override
  public void handle(byte[] data) {
    try {
      for (NIOHandler handler : handlers) {
        handler.handle(data);
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
    // 回收接收缓存
    BufferPool.getInstance().recycle(readBuffer);

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
