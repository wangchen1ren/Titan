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
 * From server to client in response to command, if no error and no result set.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0
 * 1-9 (Length Coded Binary)   affected_rows
 * 1-9 (Length Coded Binary)   insert_id
 * 2                           server_status
 * 2                           warning_count
 * n   (until end of packet)   message fix:(Length Coded String)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#OK_Packet
 * </pre>
 * 
 */
public class OkPacket extends MySQLPacket {
  public static final byte FIELD_COUNT = 0x00;
  public static final byte[] OK = new byte[] { 7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };

  public byte fieldCount = FIELD_COUNT;
  public long affectedRows;
  public long insertId;
  public int serverStatus;
  public int warningCount;
  public byte[] message;

  public OkPacket() {
  }

  public OkPacket(byte packetId,
      long affectedRows,
      long insertId,
      int serverStatus,
      int warningCount,
      byte[] message) {
    this.packetId = packetId;
    this.affectedRows = affectedRows;
    this.insertId = insertId;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.message = message;
  }

  public void read(byte[] data) {
    MySQLMessage mm = new MySQLMessage(data);
    packetLength = mm.readUB3();
    packetId = mm.read();
    fieldCount = mm.read();
    affectedRows = mm.readLength();
    insertId = mm.readLength();
    serverStatus = mm.readUB2();
    warningCount = mm.readUB2();
    if (mm.hasRemaining()) {
      this.message = mm.readBytesWithLength();
    }
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtils.writeUB3(buffer, size - PACKET_HEADER_SIZE);
    buffer.put(packetId);
    buffer.put(fieldCount);
    BufferUtils.writeLength(buffer, affectedRows);
    BufferUtils.writeLength(buffer, insertId);
    BufferUtils.writeUB2(buffer, serverStatus);
    BufferUtils.writeUB2(buffer, warningCount);
    if (message != null) {
      BufferUtils.writeWithLength(buffer, message);
    }
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // fieldCount
    size += BufferUtils.getLength(affectedRows); // affectedRows
    size += BufferUtils.getLength(insertId); // insertId
    size += 2; // serverStatus
    size += 2; // warningCount
    if (message != null) {
      size += BufferUtils.getLength(message);
    }
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL OK Packet";
  }

}
