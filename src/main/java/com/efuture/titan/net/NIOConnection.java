package com.efuture.titan.net;

import java.io.IOException;
import java.nio.channels.Selector;

public interface NIOConnection {

  /**
   * 注册网络事件
   */
  void register(NIOProcessor processor, Selector selector) throws IOException;

  /**
   * 读取数据
   */
  void read() throws IOException;

  /**
   * 处理数据
   */
  void handle(byte[] data);

  /**
   * 写出一块数据
   */
  void write(byte[] data);

  void writeByQueue() throws IOException;

  /**
   * Close
   */
  void close();
}
