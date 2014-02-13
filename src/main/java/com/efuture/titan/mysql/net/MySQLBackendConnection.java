
package com.efuture.titan.mysql.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.Capabilities;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.ExecuteThreadPool;
import com.efuture.titan.metastore.DataSource;
import com.efuture.titan.mysql.net.packet.AuthPacket;
import com.efuture.titan.mysql.net.packet.BinaryPacket;
import com.efuture.titan.mysql.net.packet.EOFPacket;
import com.efuture.titan.mysql.net.packet.ErrorPacket;
import com.efuture.titan.mysql.net.packet.HandshakePacket;
import com.efuture.titan.mysql.net.packet.OkPacket;
import com.efuture.titan.mysql.net.packet.Reply323Packet;
import com.efuture.titan.mysql.net.packet.QuitPacket;
import com.efuture.titan.mysql.security.SecurityUtils;
import com.efuture.titan.net.bio.Connection;
import com.efuture.titan.util.CharsetUtils;
import com.efuture.titan.util.StreamUtils;
import com.efuture.titan.util.StringUtils;

public class MySQLBackendConnection implements Connection {
  private static final Log LOG = LogFactory.getLog(MySQLBackendConnection.class);

  private static final long DEFAULT_WAIT_TIMEOUT = 10 * 1000L;
  private static final int RECV_BUFFER_SIZE = 16 * 1024;
  private static final int SEND_BUFFER_SIZE = 8 * 1024;
  private static final int INPUT_STREAM_BUFFER = 16 * 1024;
  private static final int OUTPUT_STREAM_BUFFER = 8 * 1024;
  private static final int SOCKET_CONNECT_TIMEOUT = 10 * 1000;
  private static final long MAX_PACKET_SIZE = 1024 * 1024 * 16;

  private static final long CLIENT_FLAGS = getClientFlags();
  private static long getClientFlags() {
    long flag = 0;
    flag |= Capabilities.CLIENT_LONG_PASSWORD;
    flag |= Capabilities.CLIENT_FOUND_ROWS;
    flag |= Capabilities.CLIENT_LONG_FLAG;
    flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
    // flag |= Capabilities.CLIENT_NO_SCHEMA;
    // flag |= Capabilities.CLIENT_COMPRESS;
    flag |= Capabilities.CLIENT_ODBC;
    // flag |= Capabilities.CLIENT_LOCAL_FILES;
    flag |= Capabilities.CLIENT_IGNORE_SPACE;
    flag |= Capabilities.CLIENT_PROTOCOL_41;
    flag |= Capabilities.CLIENT_INTERACTIVE;
    // flag |= Capabilities.CLIENT_SSL;
    flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
    flag |= Capabilities.CLIENT_TRANSACTIONS;
    // flag |= Capabilities.CLIENT_RESERVED;
    flag |= Capabilities.CLIENT_SECURE_CONNECTION;
    // client extension
    // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
    // flag |= Capabilities.CLIENT_MULTI_RESULTS;
    return flag;
  } 

  private TitanConf conf;

  private String host;
  private int port;
  private String username;
  private String password;
  private String database;
  private long timeout;

  private Socket socket;
  private int charsetIndex;
  private String charset;
  private InputStream in;
  private OutputStream out;

  private boolean isClosed = true;

  public MySQLBackendConnection(TitanConf conf, DataSource dataSource) {
    this(conf, dataSource, DEFAULT_WAIT_TIMEOUT);
  }

  public MySQLBackendConnection(TitanConf conf, DataSource dataSource, long timeout) {
    this.conf = conf;
    this.host = dataSource.getHost();
    this.port = dataSource.getPort();
    this.username = dataSource.getUsername();
    this.password = dataSource.getPassword();
    this.database = dataSource.getDatabase();
    this.timeout = timeout;
  }

  public void connect() throws Exception {
    socket = new Socket();
    socket.setTcpNoDelay(true);
    socket.setTrafficClass(0x04 | 0x10);
    socket.setPerformancePreferences(0, 2, 1); 
    socket.setReceiveBufferSize(RECV_BUFFER_SIZE);
    socket.setSendBufferSize(SEND_BUFFER_SIZE);
    socket.connect(new InetSocketAddress(host, port), SOCKET_CONNECT_TIMEOUT);
    in = new BufferedInputStream(socket.getInputStream(), INPUT_STREAM_BUFFER);
    out = new BufferedOutputStream(socket.getOutputStream(), OUTPUT_STREAM_BUFFER);

    isClosed = false;

    // 完成连接和初始化
    FutureTask<Connection> ft = new FutureTask<Connection>(new Callable<Connection>() {
      @Override
      public Connection call() throws Exception {
        handshake();
        return null;
      }
    });
    ExecuteThreadPool.get(conf).execute(ft);
    try {
      ft.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      ft.cancel(true);
      throw e;
    }
  }

  private void handshake() throws IOException {
    BinaryPacket initPacket = receive();
    HandshakePacket handshakePacket = new HandshakePacket();
    handshakePacket.read(initPacket.getBytes());

    int ci = handshakePacket.serverCharsetIndex & 0xff;
    this.charset = CharsetUtils.getCharset(ci);
    if (charset != null) {
      charsetIndex = ci;
    } else {
      throw new UnknownCharsetException("charset:" + ci);
    }

    try {
      BinaryPacket bin = sendAuth411(handshakePacket);
      switch (bin.data[0]) {
        case OkPacket.FIELD_COUNT:
          afterConnectSuccess();
          break;
        case ErrorPacket.FIELD_COUNT:
          ErrorPacket err = new ErrorPacket();
          err.read(bin.getBytes());
          throw new ErrorPacketException(new String(err.message, charset));
        case EOFPacket.FIELD_COUNT:
          auth323(bin.packetId, handshakePacket.seed);
          break;
        default:
          throw new UnknownPacketException(bin.toString());
      }   
    //} catch (NoSuchAlgorithmException e) {
    //  throw new IllegalArgumentException(e.getMessage());
    } catch (IOException e) {
      throw e;
    }
  }

  private BinaryPacket sendAuth411(HandshakePacket handshakePacket) throws IOException {
    try {
      byte[] passwd = null;
      if (password != null && password.length() > 0) {
        try {
          passwd = password.getBytes(charset);
          byte[] seed = handshakePacket.seed;
          byte[] restOfScramble = handshakePacket.restOfScrambleBuff;
          byte[] authSeed = new byte[seed.length + restOfScramble.length];
          System.arraycopy(seed, 0, authSeed, 0, seed.length);
          System.arraycopy(restOfScramble, 0, authSeed, seed.length, restOfScramble.length);
          passwd = SecurityUtils.scramble411(passwd, authSeed);
        } catch (UnsupportedEncodingException e) {
          // ignore
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
      AuthPacket ap = new AuthPacket((byte) 1, CLIENT_FLAGS,
          MAX_PACKET_SIZE, charsetIndex, null,
          username, passwd, database);
      return write(ap.getBytes());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  /**
   * 323协议认证
   */
  private void auth323(byte packetId, byte[] seed) throws IOException {
    byte[] r323Seed = null;
    if (password != null && password.length() > 0) {
      r323Seed = SecurityUtils.scramble323(password, new String(seed)).getBytes();
    }
    Reply323Packet r323 = new Reply323Packet(++packetId, r323Seed);

    BinaryPacket bin = write(r323.getBytes());
    switch (bin.data[0]) {
      case OkPacket.FIELD_COUNT:
        afterConnectSuccess();
        break;
      case ErrorPacket.FIELD_COUNT:
        ErrorPacket err = new ErrorPacket();
        err.read(bin.getBytes());
        throw new ErrorPacketException(new String(err.message, charset));
      default:
        throw new UnknownPacketException(bin.toString());
    }
  }

  private void afterConnectSuccess() {
  }

  public String getCharset() {
    return charset;
  }

  public BinaryPacket receive() throws IOException {
    int packetLength = StreamUtils.readUB3(in);
    byte packetId = StreamUtils.read(in);
    byte[] data = StreamUtils.read(in, packetLength);
    return new BinaryPacket(packetId, data);
  }

  public BinaryPacket write(byte[] data) throws IOException {
    out.write(data);
    out.flush();
    return receive();
  }

  /** 
   * 关闭连接之前先尝试发送quit数据包
   */
  public void close() {
    try {
      if (out != null) {
        out.write(QuitPacket.QUIT);
        out.flush();
      }
    } catch (IOException e) {
      LOG.error("Failed to send QUIT packet to mysql", e); 
    } finally {
      try {
        socket.close();
      } catch (Throwable e) {
        LOG.error("Failed to close socket", e); 
      }
    }
  }

  public void release() {
    close();
  }
}
