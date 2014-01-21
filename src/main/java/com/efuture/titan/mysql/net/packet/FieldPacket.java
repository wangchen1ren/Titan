/*
 * Copyright 2012-2015 org.opencloudb.
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

import com.efuture.titan.util.BufferUtil;

/**
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * 
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
 * </pre>
 * 
 */
public class FieldPacket extends MySQLPacket {
  private static final byte[] DEFAULT_CATALOG = "def".getBytes();
  private static final byte[] FILLER = new byte[2];

  public byte[] catalog = DEFAULT_CATALOG;
  public byte[] db;
  public byte[] table;
  public byte[] orgTable;
  public byte[] name;
  public byte[] orgName;
  public int charsetIndex;
  public long length;
  public int type;
  public int flags;
  public byte decimals;
  public byte[] definition;

  public FieldPacket(byte packetId,
      byte[] name,
      int charsetIndex,
      int type) {
    this.name = name;
    this.charsetIndex = charsetIndex;
    this.type = type;
  }

  public FieldPacket(byte packetId,
      byte[] name,
      byte[] orgName,
      int charsetIndex,
      int type) {
    this.name = name;
    this.orgName = orgName;
    this.charsetIndex = charsetIndex;
    this.type = type;
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtil.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
    byte nullVal = 0;
    BufferUtil.writeWithLength(buffer, catalog, nullVal);
    BufferUtil.writeWithLength(buffer, db, nullVal);
    BufferUtil.writeWithLength(buffer, table, nullVal);
    BufferUtil.writeWithLength(buffer, orgTable, nullVal);
    BufferUtil.writeWithLength(buffer, name, nullVal);
    BufferUtil.writeWithLength(buffer, orgName, nullVal);
    buffer.put((byte) 0x0C);
    BufferUtil.writeUB2(buffer, charsetIndex);
    BufferUtil.writeUB4(buffer, length);
    buffer.put((byte) (type & 0xff));
    BufferUtil.writeUB2(buffer, flags);
    buffer.put(decimals);
    buffer.position(buffer.position() + FILLER.length);
    if (definition != null) {
      BufferUtil.writeWithLength(buffer, definition);
    }
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // packetId
    size += (catalog == null ? 1 : BufferUtil.getLength(catalog));
    size += (db == null ? 1 : BufferUtil.getLength(db));
    size += (table == null ? 1 : BufferUtil.getLength(table));
    size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
    size += (name == null ? 1 : BufferUtil.getLength(name));
    size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
    size += 1; // 0x0C
    size += 2; // charsetIndex
    size += 4; // length
    size += 1; // type
    size += 2; // flags
    size += 1; // decimals
    size += 2; // FILLER
    if (definition != null) {
      size += BufferUtil.getLength(definition);
    }
    return size;
  }

  @Override
  protected String getPacketInfo() {
    return "MySQL Field Packet";
  }
}
