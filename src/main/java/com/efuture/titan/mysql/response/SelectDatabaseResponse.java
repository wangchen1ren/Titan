
package com.efuture.titan.mysql.response;

import java.nio.ByteBuffer;

import com.alibaba.cobar.config.Fields;

import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.EOFPacket;
import com.efuture.titan.mysql.net.packet.FieldPacket;
import com.efuture.titan.mysql.net.packet.PacketUtils;
import com.efuture.titan.mysql.net.packet.ResultSetHeaderPacket;
import com.efuture.titan.mysql.net.packet.RowDataPacket;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.util.StringUtils;

public class SelectDatabaseResponse extends Response {
  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header;
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof;
  static {
    byte packetId = 0;
    header = new ResultSetHeaderPacket(packetId++, FIELD_COUNT);
    for (int i = 0; i < FIELD_COUNT; ++i) {
      fields[i] = PacketUtils.getField(packetId++, "DATABASE()", Fields.FIELD_TYPE_VAR_STRING);
    }
    eof = new EOFPacket(packetId++);
  }

  @Override
  public void response() {
    // header
    conn.write(header.getBytes());

    // fields
    for (FieldPacket field : fields) {
      conn.write(field.getBytes());
    }

    // eof
    conn.write(eof.getBytes());

    // rows
    byte packetId = (byte) (eof.packetId + 1);
    RowDataPacket row = new RowDataPacket(packetId++, FIELD_COUNT);
    MySQLSessionState ss = MySQLSessionState.get(conn);
    row.add(StringUtils.encodeString(ss.db, ss.charset));
    conn.write(row.getBytes());

    // eof
    EOFPacket lastEOF = new EOFPacket(packetId++);
    conn.write(lastEOF.getBytes());
  }
}
