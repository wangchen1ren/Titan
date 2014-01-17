
package com.efuture.titan.mysql.parse;

public class ParseUtil {

  public static String removeComment(String sql) {
    // pound
    int idx = -1;
    while ((idx = sql.indexOf('#')) >= 0) {
      sql = cutString(sql, idx, sql.indexOf('\n', idx));
    }
    // double-dash
    while ((idx = findNext(sql, "--")) >= 0) {
      sql = cutString(sql, idx, sql.indexOf('\n', idx));
    }
    // /* */
    while ((idx = findNext(sql, "/*")) >= 0) {
      sql = cutString(sql, idx, findNext(sql, "*/") + 1);
    }
    return sql;
  }

  private static int findNext(String str, String pattern) {
    // kmp
    int i = 0, j = 0;
    char[] t = str.toCharArray();
    char[] p = pattern.toCharArray();
    for (; i < t.length && j < p.length; ++i) {
      j = (t[i] == p[j]) ? j + 1 : 0;
    }
    if (j >= p.length) {
      return i - j;
    } else { 
      return -1;
    }
  }

  private static String cutString(String str, int start, int end) {
    StringBuilder sb = new StringBuilder();
    sb.append(str.substring(0, start - 1));
    if (start < end) {
      sb.append(str.substring(end + 1));
    }
    return sb.toString();
  }
}
