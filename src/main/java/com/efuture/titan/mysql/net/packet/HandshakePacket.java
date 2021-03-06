/*
 * Copyright 2012-2015 com.efuture.titan.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.efuture.titan.mysql.net.packet;

import java.nio.ByteBuffer;

import com.efuture.titan.mysql.net.MySQLMessage;
import com.efuture.titan.util.BufferUtils;

/**
 * From server to client during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 * 
 */
public class HandshakePacket extends MySQLPacket {
  private static final byte[] FILLER_13 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

  public byte protocolVersion;
  public byte[] serverVersion;
  public long threadId;
  public byte[] seed;
  public int serverCapabilities;
  public byte serverCharsetIndex;
  public int serverStatus;
  public byte[] restOfScrambleBuff;

  public HandshakePacket() {
  }

  public HandshakePacket(byte packetId,
      byte protocolVersion,
      byte[] serverVersion,
      long threadId,
      byte[] seed,
      int serverCapabilities,
      byte serverCharsetIndex,
      int serverStatus,
      byte[] restOfScrambleBuff) {
    this.packetId = packetId;
    this.protocolVersion = protocolVersion;
    this.serverVersion = serverVersion;
    this.threadId = threadId;
    this.seed = seed;
    this.serverCapabilities = serverCapabilities;
    this.serverCharsetIndex = serverCharsetIndex;
    this.serverStatus = serverStatus;
    this.restOfScrambleBuff = restOfScrambleBuff;
  }

  public void read(byte[] data) {
    MySQLMessage mm = new MySQLMessage(data);
    packetLength = mm.readUB3();
    packetId = mm.read();
    protocolVersion = mm.read();
    serverVersion = mm.readBytesWithNull();
    threadId = mm.readUB4();
    seed = mm.readBytesWithNull();
    serverCapabilities = mm.readUB2();
    serverCharsetIndex = mm.read();
    serverStatus = mm.readUB2();
    mm.move(13);
    restOfScrambleBuff = mm.readBytesWithNull();
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtils.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    buffer.put(protocolVersion);
    BufferUtils.writeWithNull(buffer, serverVersion);
    BufferUtils.writeUB4(buffer, threadId);
    BufferUtils.writeWithNull(buffer, seed);
    BufferUtils.writeUB2(buffer, serverCapabilities);
    buffer.put(serverCharsetIndex);
    BufferUtils.writeUB2(buffer, serverStatus);
    buffer.put(FILLER_13);
    BufferUtils.writeWithNull(buffer, restOfScrambleBuff);
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // protocolVersion
    size += serverVersion.length; // serverVersion
    size += 4; // threadId
    size += seed.length + 1; // seed
    size += 2; //serverCapabilities
    size += 1; // serverCharsetIndex
    size += 2; // serverStatus
    size += 13; // FILLER_13
    size += restOfScrambleBuff.length + 1;// restOfScrambleBuff
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Handshake Packet";
  }

}
