
package com.efuture.titan.mysql.parse;

import junit.framework.TestCase;

import com.efuture.titan.util.StringUtils;

public class ParseSelectTest extends TestCase {

  public void testParseSelect() {
    try {
      String sql;

      // Normal
      sql = "select database() ";
      assertEquals(ParseSelect.DATABASE, ParseSelect.parse(sql));
      sql = "select user()";
      assertEquals(ParseSelect.USER, ParseSelect.parse(sql));
      sql = "select current_user()";
      assertEquals(ParseSelect.USER, ParseSelect.parse(sql));
      sql = "/* */  select version() ";
      assertEquals(ParseSelect.VERSION, ParseSelect.parse(sql));
      sql = "SELECT LAST_INSERT_ID()";
      assertEquals(ParseSelect.LAST_INSERT_ID, ParseSelect.parse(sql));
      sql = "SELECT LAST_INSERT_ID() AS id";
      assertEquals(ParseSelect.LAST_INSERT_ID, ParseSelect.parse(sql));
      sql = "SELECT @@iDentity";
      assertEquals(ParseSelect.IDENTITY, ParseSelect.parse(sql));
      sql = "SELECT @@version_comment";
      assertEquals(ParseSelect.VERSION_COMMENT, ParseSelect.parse(sql));
      sql = "select @@session.auto_increment_increment";
      assertEquals(ParseSelect.SESSION_INCREMENT, ParseSelect.parse(sql));
      sql = "select @@session.tx_isolation";
      assertEquals(ParseSelect.SESSION_ISOLATION, ParseSelect.parse(sql));

      // Other
      sql = "/* */  select version(1.0.0) ";
      assertEquals(ParseSelect.OTHER, ParseSelect.parse(sql));
    } catch (Exception e) {
      fail("Error in test parse select" +
          StringUtils.stringifyException(e));
    }
  }
}
