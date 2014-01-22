
package com.efuture.titan.mysql.processor;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.OkPacket;

public class CommentProcessor implements CommandProcessor {

  @Override
  public void init() {
  }

  @Override
  public void run(String sql, MySQLFrontendConnection conn) {
    conn.write(OkPacket.OK);
  }
}