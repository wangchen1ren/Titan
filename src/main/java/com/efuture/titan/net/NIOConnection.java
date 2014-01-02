package com.efuture.titan.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;

public interface NIOConnection extends ClosableConnection{

  /**
   * 注册网络事件
   */
  void register(Selector selector) throws IOException;

  /**
   * 读取数据
   */
  void read() throws IOException;

  /**
   * 处理数据
   */
  void handle(byte[] data);

  /**
   * 写出一块缓存数据
   */
  void write(ByteBuffer buffer);

  /**
   * 通知Queue缓存为空，可以继续写数据了
   * 
   */
  void writeQueueAvailable();

  /**
   * 通知write Queue已经满了
   * 
   */
  void writeQueueBlocked();

  /**
   * 基于处理器队列的方式写数据
   */
  void writeByQueue() throws IOException;

  /**
   * 基于监听事件的方式写数据
   */
  void writeByEvent() throws IOException;

  /**
   * 发生错误
   */
  void error(int errCode, Throwable t);


}
