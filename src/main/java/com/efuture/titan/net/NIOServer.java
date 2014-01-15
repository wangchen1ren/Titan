
package com.efuture.titan.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.util.StringUtils;

public class NIOServer extends Thread {
  private static final Log LOG = LogFactory.getLog(NIOServer.class);

  private AtomicBoolean stopped = new AtomicBoolean(false);
  private FrontendConnectionFactory factory;
  private NIOProcessorManager processorMgr;
  private Selector selector;
  private ServerSocketChannel serverChannel;

  public NIOServer(String name, int port,
      FrontendConnectionFactory factory,
      NIOProcessorManager processorMgr) throws IOException {
    super(name);
    this.factory = factory;
    this.processorMgr = processorMgr;
    this.selector = Selector.open();
    this.serverChannel = ServerSocketChannel.open();
    this.serverChannel.socket().bind(new InetSocketAddress(port));
    this.serverChannel.configureBlocking(false);
    this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
  }

  public InetAddress getAddress() {
    return serverChannel.socket().getInetAddress();
  }

  public void shutdown() {
    stopped.set(true);
    close();
    this.interrupt();
  }

  public void close() {
    try {
      this.serverChannel.close();
      this.selector.close();
    } catch (IOException e) {
      // ignore
    }
  }

  public void run() {
    try {
      while (! stopped.get()) {
        listen();
      }
    } catch (Exception e) {
      LOG.warn("Error in listening. " +
          StringUtils.stringifyException(e));
    }
  }

  private void listen() {
    while (! stopped.get()) {
      try {
        selector.select(1000L);
        Set<SelectionKey> keys = selector.selectedKeys();
        try {
          Iterator it = keys.iterator();
          while (it.hasNext()) {
            SelectionKey key = (SelectionKey) it.next();
            it.remove();
            if (key.isValid() && key.isAcceptable()) {
              accept(key);
            } else {
              key.cancel();
            }
          }
        } finally {
          keys.clear();
        }
      } catch (Exception e) {
        LOG.warn("Error in listening. " +
            StringUtils.stringifyException(e));
      }
    }
  }

  private void accept(SelectionKey key) {
    ServerSocketChannel server = (ServerSocketChannel) key.channel();
    SocketChannel channel = null;
    try {
      channel = server.accept(); 
      //channel = serverChannel.accept();
      channel.configureBlocking(false); 
      LOG.debug("Accept new channel: " + channel);
      
      NIOProcessor processor = processorMgr.nextProcessor();
      FrontendConnection conn = factory.create(channel);
      processor.register(conn);
    } catch (Exception e) {
      LOG.warn("Error in accepting. " +
          StringUtils.stringifyException(e));
      closeChannel(channel);
    }
  }

  private void closeChannel(SocketChannel channel) {
    if (channel == null) {
      return;
    }
    Socket socket = channel.socket();
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        // ignore
      }
    }
    try {
      channel.close();
    } catch (IOException e) {
      // ignore
    }
  }

}
