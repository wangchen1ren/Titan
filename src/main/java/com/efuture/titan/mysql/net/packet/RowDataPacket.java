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
import java.util.ArrayList;
import java.util.List;

import com.efuture.titan.util.BufferUtil;

/**
 * From server to client. One packet for each row in the result set.
 * 
 * <pre>
 * Bytes                   Name
 * -----                   ----
 * n (Length Coded String) (column value)
 * ...
 * 
 * (column value):         The data in the column, as a character string.
 *                         If a column is defined as non-character, the
 *                         server converts the value into a character
 *                         before sending it. Since the value is a Length
 *                         Coded String, a NULL can be represented with a
 *                         single byte containing 251(see the description
 *                         of Length Coded Strings in section "Elements" above).
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
 * </pre>
 * 
 */
public class RowDataPacket extends MySQLPacket {
	private static final byte NULL_MARK = (byte) 251;

	public final int fieldCount;
	public final List<byte[]> fieldValues;

	public RowDataPacket(byte packetId,
      int fieldCount) {
		this.fieldCount = fieldCount;
		this.fieldValues = new ArrayList<byte[]>(fieldCount);
	}

  public void add(byte[] value) {
    fieldValues.add(value);
  }

  @Override
  public byte[] getBytes() {
    int size = getPacketSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    BufferUtil.writeUB3(buffer, size - PACKET_HEADER_SIZE); // header
    buffer.put(packetId);
		for (int i = 0; i < fieldCount; i++) {
			byte[] v = fieldValues.get(i);
			if (v == null || v.length == 0) {
				buffer.put(RowDataPacket.NULL_MARK);
			} else {
				BufferUtil.writeLength(buffer, v.length);
			}
		}
    return buffer.array();
  }

  @Override
  public int getPacketSize() {
    int size = PACKET_HEADER_SIZE;
    size += 1; // packetId
		for (int i = 0; i < fieldCount; i++) {
			byte[] v = fieldValues.get(i);
			size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
		}
		return size;
  }

	@Override
	protected String getPacketInfo() {
		return "MySQL RowData Packet";
	}

}
