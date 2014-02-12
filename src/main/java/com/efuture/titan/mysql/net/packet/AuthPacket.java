
package com.efuture.titan.mysql.net.packet;

import java.nio.ByteBuffer;

import com.alibaba.cobar.config.Capabilities;

import com.efuture.titan.mysql.net.MySQLMessage;
import com.efuture.titan.util.BufferUtil;

/**
 * From client to server during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 * 
 */
public class AuthPacket extends MySQLPacket {
  public static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
  public static final byte[] FILLER = new byte[23];

  public long clientFlags;
  public long maxPacketSize;
  public int charsetIndex;
  public byte[] extra;// from FILLER(23)
  public String user;
  public byte[] password;
  public String database;

  public AuthPacket() {
  }

  public AuthPacket(byte packetId,
      long clientFlags,
      long maxPacketSize,
      int charsetIndex,
      byte[] extra,
      String user,
      byte[] password,
      String database) {
    this.packetId = packetId;
    this.clientFlags = clientFlags;
    this.maxPacketSize = maxPacketSize;
    this.charsetIndex = charsetIndex;
    this.extra = extra;
    this.user = user;
    this.password = password;
    this.database = database;
  }

  public void read(byte[] data) {
    MySQLMessage mm = new MySQLMessage(data);
    packetLength = mm.readUB3();
    packetId = mm.read();
    clientFlags = mm.readUB4();
    maxPacketSize = mm.readUB4();
    charsetIndex = (mm.read() & 0xff);
    // read extra
    int current = mm.position();
    int len = (int) mm.readLength();
    if (len > 0 && len < FILLER.length) {
      byte[] ab = new byte[len];
      System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
      this.extra = ab;
    }
    mm.position(current + FILLER.length);
    user = mm.readStringWithNull();
    password = mm.readBytesWithLength();
    if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && mm.hasRemaining()) {
      database = mm.readStringWithNull();
    }
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtil.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    BufferUtil.writeUB4(buffer, clientFlags);
    BufferUtil.writeUB4(buffer, maxPacketSize);
    buffer.put((byte) charsetIndex);
    buffer.put(FILLER);
    if (user == null) {
      buffer.put((byte) 0);
    } else {
      byte[] userData = user.getBytes();
      BufferUtil.writeWithNull(buffer, userData);
    }
    if (password == null) {
      buffer.put((byte) 0);
    } else {
      BufferUtil.writeWithLength(buffer, password);
    }
    if (database == null) {
      buffer.put((byte) 0);
    } else {
      byte[] databaseData = database.getBytes();
      BufferUtil.writeWithNull(buffer, databaseData);
    }
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 4; // clientFlags
    size += 4; // maxPacketSize
    size += 1; // charsetIndex
    size += 23; // FILLER
    size += (user == null) ? 1 : user.length() + 1; // user
    size += (password == null) ? 1 : BufferUtil.getLength(password); // password
    size += (database == null) ? 1 : database.length() + 1; // database
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Authentication Packet";
  }

}
