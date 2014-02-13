
package com.efuture.titan.mysql.net.packet;

import java.io.UnsupportedEncodingException;

import com.efuture.titan.util.CharsetUtils;
import com.efuture.titan.util.StringUtils;

public class PacketUtils {
  private static final String CODE_PAGE_1252 = "Cp1252";

  public static final FieldPacket getField(byte packetId,
      String name, int type) {
    FieldPacket packet = new FieldPacket(packetId,
        StringUtils.encodeString(name, CODE_PAGE_1252),
        CharsetUtils.getIndex(CODE_PAGE_1252),
        (byte) type
        );
    return packet;
  }

  public static final FieldPacket getField(byte packetId,
      String name, String orgName, int type) {
    FieldPacket packet = new FieldPacket(packetId,
        StringUtils.encodeString(name, CODE_PAGE_1252),
        StringUtils.encodeString(orgName, CODE_PAGE_1252),
        CharsetUtils.getIndex(CODE_PAGE_1252),
        (byte) type
        );
    return packet;
  }

}
