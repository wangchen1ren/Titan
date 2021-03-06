/*
 * Copyright 2012-2015 com.efuture.
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

import com.efuture.titan.util.BufferUtils;

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 * 
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
 * </pre>
 * 
 */
public class EOFPacket extends MySQLPacket {
  public static final byte FIELD_COUNT = (byte) 0xfe;

  public byte fieldCount = FIELD_COUNT;
  public int warningCount = 0;
  public int status = 2;

  public EOFPacket(byte packetId) {
    this.packetId = packetId;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtils.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    buffer.put(fieldCount);
    BufferUtils.writeUB2(buffer, warningCount);
    BufferUtils.writeUB2(buffer, status);
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // fieldCount
    size += 2; // warningCount
    size += 2; // status
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL EOF Packet";
  }

}
