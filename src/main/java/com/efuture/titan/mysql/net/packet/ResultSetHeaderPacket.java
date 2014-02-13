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
 * From server to client after command, if no error and result set -- that is,
 * if the command was a query which returned a result set. The Result Set Header
 * Packet is the first of several, possibly many, packets that the server sends
 * for result sets. The order of packets for a result set is:
 * 
 * <pre>
 * (Result Set Header Packet)   the number of columns
 * (Field Packets)              column descriptors
 * (EOF Packet)                 marker: end of Field Packets
 * (Row Data Packets)           row contents
 * (EOF Packet)                 marker: end of Data Packets
 * 
 * Bytes                        Name
 * -----                        ----
 * 1-9   (Length-Coded-Binary)  field_count
 * 1-9   (Length-Coded-Binary)  extra
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
 * </pre>
 * 
 */
public class ResultSetHeaderPacket extends MySQLPacket {

  public int fieldCount;
  public long extra;

  public ResultSetHeaderPacket(byte packetId,
      int fieldCount) {
    this(packetId, fieldCount, 0);
  }

  public ResultSetHeaderPacket(byte packetId,
      int fieldCount, long extra) {
    this.packetId = packetId;
    this.fieldCount = fieldCount;
    this.extra = extra;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtils.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    BufferUtils.writeLength(buffer, fieldCount);
    if (extra > 0) {
      BufferUtils.writeLength(buffer, extra);
    }
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += BufferUtils.getLength(fieldCount); // fieldCount
    if (extra > 0) {
      size += BufferUtils.getLength(extra); // extra
    }
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL ResultSetHeader Packet";
  }

}
