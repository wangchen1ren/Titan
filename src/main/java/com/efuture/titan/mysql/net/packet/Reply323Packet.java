
package com.efuture.titan.mysql.net.packet;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.efuture.titan.util.BufferUtils;

public class Reply323Packet extends MySQLPacket {

  public byte[] seed;

  public Reply323Packet(byte packetId,
      byte[] seed) {
    this.packetId = packetId;
    this.seed = seed;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtils.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    if (seed == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put(seed);
    }
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += (seed == null) ? 1 : seed.length + 1;
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Auth323 Packet";
  }

}
