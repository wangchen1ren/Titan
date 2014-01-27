
package com.efuture.titan.mysql.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.net.buffer.BufferPool;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.net.NIOHandler;
import com.efuture.titan.util.StringUtils;
import com.efuture.titan.util.TimeUtil;

public class MySQLFrontendConnection extends FrontendConnection {
  private static final Log LOG = LogFactory.getLog(MySQLFrontendConnection.class);

  private static final int MYSQL_PACKET_HEADER_SIZE = 4;
  private static final int MAX_PACKET_SIZE = 16 << 20;

  protected MySQLSessionState ss;

  protected ByteBuffer readBuffer;
  protected int readBufferOffset;

  public MySQLFrontendConnection(TitanConf conf, SocketChannel channel) {
    super(conf, channel);
    this.readBuffer = BufferPool.getInstance().allocate();
    this.readBufferOffset = 0;
    startSession();
  }

  @Override
  public void startSession() {
    // init sessionstate
    ss = MySQLSessionState.get(this);
  }

  public MySQLSessionState getSessionState() {
    return ss;
  }

  @Override
  public void closeSession() {
    MySQLSessionState.remove(this);
  }

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

  @Override
  public void close() {
    BufferPool.getInstance().recycle(readBuffer);
    closeSession();
    super.close();
  }

  public void writeErrMessage(int errno, String msg) {
    writeErrMessage((byte) 1, errno, msg);
  }

  public void writeErrMessage(byte id, int errno, String msg) {
    //TODO
    //ErrorPacket err = new ErrorPacket(id, errno,
    //    StringUtils.encodeString(msg, ss.charset));
  }
}
