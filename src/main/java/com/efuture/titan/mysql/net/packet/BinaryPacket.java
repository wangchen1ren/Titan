
package com.efuture.titan.mysql.net.packet;

import java.nio.ByteBuffer;

import com.efuture.titan.util.BufferUtil;

public class BinaryPacket extends MySQLPacket {
  public static final byte OK = 1;
  public static final byte ERROR = 2;
  public static final byte HEADER = 3;
  public static final byte FIELD = 4;
  public static final byte FIELD_EOF = 5;
  public static final byte ROW = 6;
  public static final byte PACKET_EOF = 7;

  public byte[] data;

  public BinaryPacket(byte packetId,
      byte[] data) {
    this.packetId = packetId;
    this.data = data;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtil.writeUB3(buffer, size - PACKET_HEADER_SIZE);
    buffer.put(packetId);
    buffer.put(data);
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += (data == null) ? 0 : data.length;
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Binary Packet";
  }

}
