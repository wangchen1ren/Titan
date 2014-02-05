
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

public class PrepareStatementResponse extends Response {

  /*
  private PrepareStatement ps;

  public void setPrepareStatement(PrepareStatement ps) {
    this.ps = ps;
  }
  */

  @Override
  public void response() {
  }
}
