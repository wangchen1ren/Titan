
package com.efuture.titan.mysql.parse;

import org.apache.commons.lang.StringUtils;

public class ParseUtil {

  public static String removeComment(String sql) {
    // pound
    int idx = -1;
    while ((idx = sql.indexOf('#')) >= 0) {
      sql = cutString(sql, idx, sql.indexOf('\n', idx));
    }
    // double-dash
    while ((idx = StringUtils.indexOf(sql, "--")) >= 0) {
      sql = cutString(sql, idx, sql.indexOf('\n', idx));
    }
    // /* */
    while ((idx = StringUtils.indexOf(sql, "/*")) >= 0) {
      sql = cutString(sql, idx, StringUtils.indexOf(sql, "*/", idx) + 1);
    }
    return sql;
  }

  private static String cutString(String str, int start, int end) {
    StringBuilder sb = new StringBuilder();
    sb.append(str.substring(0, start));
    if (start < end) {
      sb.append(str.substring(end + 1));
    }
    return sb.toString().trim();
  }
}
