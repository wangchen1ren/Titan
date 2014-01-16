
package com.efuture.titan.net;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.net.buffer.BufferQueue;
import com.efuture.titan.net.handler.NIOHandler;

public abstract class FrontendConnectionFactory {

  protected static final int SOCKET_RECV_BUFFER_SIZE = 8 * 1024;
  protected static final int SOCKET_SEND_BUFFER_SIZE = 16 * 1024;
  protected static final long DEFAULT_IDLE_TIMEOUT = 8 * 3600 * 1000L;
  protected static final String DEFAULT_CHARSET = "UTF-8";

  protected TitanConf conf;
  protected NIOHandler handler;

  public FrontendConnectionFactory(TitanConf conf) {
    this.conf = conf;
  }

  public FrontendConnection create(SocketChannel channel) throws IOException {
    Socket socket = channel.socket();
    socket.setReceiveBufferSize(SOCKET_RECV_BUFFER_SIZE);
    socket.setSendBufferSize(SOCKET_SEND_BUFFER_SIZE);
    socket.setTcpNoDelay(true);
    socket.setKeepAlive(true);

    FrontendConnection conn = getConnection(channel);
    //conn.setIdleTimeout(idleTimeout);
    //conn.setCharset(charset);

    return conn;
  }

  abstract public FrontendConnection getConnection(SocketChannel channel);

  /*
  public FrontendConnection getConnection(SocketChannel channel) {
    FrontendConnection conn = new FrontendConnection(conf, channel);
    conn.setHandler(handler);
    return conn;
  }
  */

  public void setNIOHandler(NIOHandler handler) {
    this.handler = handler;
  }

}
