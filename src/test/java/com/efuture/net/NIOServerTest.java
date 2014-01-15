
package com.efuture.titan.net;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import junit.framework.TestCase;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.util.StringUtils;

public class NIOServerTest extends TestCase {

  private static final int TEST_PORT = 20001;
  private static final String TEST_MESSAGE = "test";
  private static final int N_PACKET = 2000;

  private TitanConf conf;
  private NIOServer server;
  private NIOProcessorManager mgr;
  private TestHandler testHandler;

  private class TestHandler implements NIOHandler {

    private int calledTimes = 0;
    private byte[] data;

    @Override
    public void handle(byte[] data) {
      calledTimes++;
      //System.err.println("Got handle data: " + new String(data));
      //System.err.println("Data length: " + data.length);
      this.data = data;
    }

    public byte[] getData() {
      if (data != null) {
        //System.err.println("Handler data: " + new String(data));
      }
      return data;
    }

    public int getCalledTimes() {
      return calledTimes;
    }
  }

  public void setUp() {
    try {
      conf = new TitanConf();
      conf.setIntVar(ConfVars.TITAN_SERVER_PROCESSORS, 1);
      conf.setIntVar(ConfVars.TITAN_SERVER_EXECUTORS_PER_PROCESSOR, 1);

      testHandler = new TestHandler();
      FrontendConnectionFactory factory = new FrontendConnectionFactory(conf);
      factory.addNIOHandler(testHandler);

      mgr = new NIOProcessorManager(conf, "TManager");
      mgr.start();

      server = new NIOServer("TestServer", TEST_PORT, factory, mgr);
      server.start();

      // wait server start
      Thread.sleep(1000L);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed in setup: " + StringUtils.stringifyException(e));
    }
  }

  public void tearDown() {
    try {
      server.shutdown();
      mgr.stop();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed in teardown: " + StringUtils.stringifyException(e));
    }
  }

  public void testOnePacket() {
    try {
      System.err.println("testOnePacket");
      TestClient client = new TestClient("localhost", TEST_PORT);
      client.sendOnePacket();
      //wait
      Thread.sleep(1000L);
      assertEquals(TEST_MESSAGE, getMessage(testHandler.getData()));
      client.close();
    } catch (Exception e) {
      fail("Failed in testOnePacket: " + StringUtils.stringifyException(e));
    }
  }

  public void testMultiPacket() {
    try {
      System.err.println("testMultiPacket");
      TestClient client = new TestClient("localhost", TEST_PORT);
      client.sendMultiPacket();
      //wait
      Thread.sleep(5000L);
      assertEquals(N_PACKET, testHandler.getCalledTimes());
      assertEquals(TEST_MESSAGE, getMessage(testHandler.getData()));
      client.close();
    } catch (Exception e) {
      fail("Failed in testOnePacket: " + StringUtils.stringifyException(e));
    }
  }

  private String getMessage(byte[] data) {
    if (data == null) {
      return new String();
    }
    int offset = 0;
    int length = data[offset] & 0xff;
    length |= (data[++offset] & 0xff) << 8;
    length |= (data[++offset] & 0xff) << 16; 

    return new String(data, 4, length);
  }

  private ByteBuffer getTestPacketBuffer() {
    int len = TEST_MESSAGE.length();
    ByteBuffer buff = ByteBuffer.allocate(4 + len);
    buff.put((byte) (len & 0xff));
    buff.put((byte) (len >>> 8));
    buff.put((byte) (len >>> 16));
    buff.put((byte) 0);
    buff.put(TEST_MESSAGE.getBytes());
    return buff;
  }

  private class TestClient {
    private SocketChannel channel;

    public TestClient(String host, int port) throws Exception {
      channel = SocketChannel.open(new InetSocketAddress(host, port));
      channel.configureBlocking(false);
    }

    public void sendOnePacket() throws IOException {
      ByteBuffer buff = getTestPacketBuffer();
      buff.flip();
      int sent = channel.write(buff); 
      //System.err.println("Sent: " + sent);
    }

    public void sendMultiPacket() throws IOException {
      for (int i = 0; i < N_PACKET; ++i) {
        sendOnePacket();
      }
    }

    public void close() throws IOException {
      if (channel != null) {
        channel.close();
      }
    }
  }
}
