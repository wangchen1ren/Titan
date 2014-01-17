
package com.efuture.titan.mysql.exec;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.OkPacket;

public class CommentProcessor implements Processor {

  @Override
  public void process(String sql, MySQLFrontendConnection conn) {
    conn.write(OkPacket.OK);
  }
}
