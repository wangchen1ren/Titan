
package com.efuture.titan.mysql.parse;

import junit.framework.TestCase;

public class ParseUtilTest extends TestCase {
  
  public testRemoveComment() {
    String sql = "abcd#ddd\ne";
    assertEquals(new String("abcde"), ParseUtil.removeComment(sql));
    sql = "-- ffaddfa";
    assertEquals(new String(), ParseUtil.removeComment(sql));
    sql = "/*f adasdfa \n fdakdfl \n */ select";
    assertEquals(new String("select"), ParseUtil.removeComment(sql));
  }
}
