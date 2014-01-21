
package com.efuture.titan.mysql.response;

import java.nio.ByteBuffer;

import com.efuture.titan.common.Fields;
import com.efuture.titan.common.Versions;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.EOFPacket;
import com.efuture.titan.mysql.net.packet.FieldPacket;
import com.efuture.titan.mysql.net.packet.PacketUtils;
import com.efuture.titan.mysql.net.packet.ResultSetHeaderPacket;
import com.efuture.titan.mysql.net.packet.RowDataPacket;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.util.StringUtils;

public class SessionIsolationResponse extends Response {
  private static final int FIELD_COUNT = 1;
  private static final ResultSetHeaderPacket header;
  private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
  private static final EOFPacket eof;
  static {
    byte packetId = 0;
    header = new ResultSetHeaderPacket(packetId++, FIELD_COUNT);
    for (int i = 0; i < FIELD_COUNT; ++i) {
      fields[i] = PacketUtils.getField(packetId++,
          "@@session.tx_isolation", Fields.FIELD_TYPE_STRING);
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

    // row
    byte packetId = (byte) (eof.packetId + 1);
    RowDataPacket row = new RowDataPacket(packetId++, FIELD_COUNT);
    MySQLSessionState ss = MySQLSessionState.get(conn);
    row.add(StringUtils.encodeString("REPEATABLE-READ", ss.charset));
    conn.write(row.getBytes());

    // eof
    EOFPacket lastEOF = new EOFPacket(packetId++);
    conn.write(lastEOF.getBytes());
  }

}
