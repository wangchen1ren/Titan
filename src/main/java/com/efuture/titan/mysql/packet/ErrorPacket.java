package com.efuture.titan.mysql.packet;

import java.nio.ByteBuffer;

import com.efuture.titan.util.BufferUtil;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 */
public class ErrorPacket extends MySQLPacket {
  public static final byte FIELD_COUNT = (byte) 0xff;
  private static final byte SQLSTATE_MARKER = (byte) '#';
  private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

  public byte fieldCount = FIELD_COUNT;
  public int errno;
  public byte mark = SQLSTATE_MARKER;
  public byte[] sqlState = DEFAULT_SQLSTATE;
  public byte[] message;

  public ErrorPacket(byte packetId, int errno, byte[] message) {
    this.packetId = packetId;
    this.errno = errno;
    this.message = message;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtil.writeUB3(buffer, size - PACKET_HEADER_SIZE);
    buffer.put(packetId);
    buffer.put(fieldCount);
    BufferUtil.writeUB2(buffer, errno);
    buffer.put(mark);
    buffer.put(sqlState);
    buffer.put(message);
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // packetId
    size += 1; // fieldCount
    size += 2; // errno
    size += 1; // mark
    size += 5; // sqlState
    size += message.length;
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Error Packet";
  }

}
