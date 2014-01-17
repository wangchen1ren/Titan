
package com.efuture.titan.mysql.exec;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.parse.ParseSelect;

public class SelectProcessor implements Processor {

  @Override
  public void process(String sql, MySQLFrontendConnection conn) {
    //TODO
    int selectOp = ParseSelect.parse(sql);

  }
}
