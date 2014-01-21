
package com.efuture.titan.mysql.response;

import com.efuture.titan.mysql.net.packet.OkPacket;

public class PingResponse extends Response {
  
  @Override
  public void response() {
    conn.write(OkPacket.OK);
  }
}
