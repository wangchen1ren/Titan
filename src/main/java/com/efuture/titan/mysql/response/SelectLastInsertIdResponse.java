
package com.efuture.titan.mysql.response;

import java.nio.ByteBuffer;

import com.efuture.titan.common.Fields;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.EOFPacket;
import com.efuture.titan.mysql.net.packet.FieldPacket;
import com.efuture.titan.mysql.net.packet.PacketUtils;
import com.efuture.titan.mysql.net.packet.ResultSetHeaderPacket;
import com.efuture.titan.mysql.net.packet.RowDataPacket;
import com.efuture.titan.mysql.parse.ParseUtil;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.util.LongUtil;
import com.efuture.titan.util.StringUtils;

public class SelectLastInsertIdResponse extends Response {
  private static final String ORG_NAME = "LAST_INSERT_ID()";
  private static final int FIELD_COUNT = 1;
  private static ResultSetHeaderPacket header;
  private static FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static EOFPacket eof;

  private String name;

  public void setFieldName(String name) {
    if (name != null) {
      this.name = name;
    } else {
      this.name = ORG_NAME;
    }
  }

  @Override
  public void response() {
    byte packetId = 0;

    // header
    header = new ResultSetHeaderPacket(packetId++, FIELD_COUNT);
    conn.write(header.getBytes());

    // fields
    for (int i = 0; i < FIELD_COUNT; ++i) {
      fields[i] = PacketUtils.getField(packetId++,
          name, ORG_NAME, Fields.FIELD_TYPE_LONGLONG);
      conn.write(fields[i].getBytes());
    }

    // eof
    eof = new EOFPacket(packetId++);
    conn.write(eof.getBytes());

    // rows
    RowDataPacket row = new RowDataPacket(packetId++, FIELD_COUNT);
    MySQLSessionState ss = MySQLSessionState.get(conn);
    row.add(LongUtil.toBytes(ss.lastInsertId));
    conn.write(row.getBytes());

    // eof
    EOFPacket lastEOF = new EOFPacket(packetId++);
    conn.write(lastEOF.getBytes());
  }
}
