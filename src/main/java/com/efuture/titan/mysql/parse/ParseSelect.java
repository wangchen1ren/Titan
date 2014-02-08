
package com.efuture.titan.mysql.parse;

import java.util.StringTokenizer;

public class ParseSelect {

  public static final int OTHER = -1; 
  public static final int VERSION_COMMENT = 1;
  public static final int DATABASE = 2;
  public static final int USER = 3;
  public static final int LAST_INSERT_ID = 4;
  public static final int IDENTITY = 5;
  public static final int VERSION = 6;

  public static int parse(String sql) {
    sql = ParseUtil.removeComment(sql);
    StringTokenizer st = new StringTokenizer(sql);
    String select = st.nextToken();
    StringBuilder sb = new StringBuilder();
    while (st.hasMoreTokens()) {
      sb.append(st.nextToken());
      sb.append(" ");
    }
    return com.alibaba.cobar.server.parser.ServerParseSelect.parse(sb.toString(), 0);
  }

}
