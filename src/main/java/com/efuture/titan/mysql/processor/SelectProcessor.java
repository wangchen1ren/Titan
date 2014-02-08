
package com.efuture.titan.mysql.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.alibaba.cobar.config.ErrorCode;
import com.efuture.titan.exec.CommandProcessor;
import com.efuture.titan.exec.Driver;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.parse.MySQLParse;
import com.efuture.titan.mysql.parse.ParseSelect;
import com.efuture.titan.mysql.response.Response;
import com.efuture.titan.mysql.response.SelectDatabaseResponse;
import com.efuture.titan.mysql.response.SelectIdentityResponse;
import com.efuture.titan.mysql.response.SelectLastInsertIdResponse;
import com.efuture.titan.mysql.response.SelectUserResponse;
import com.efuture.titan.mysql.response.SelectVersionResponse;
import com.efuture.titan.mysql.response.SelectVersionCommentResponse;
import com.efuture.titan.mysql.response.SessionIncrementResponse;
import com.efuture.titan.mysql.response.SessionIsolationResponse;
import com.efuture.titan.session.SessionState;
import com.efuture.titan.util.ReflectionUtils;

// TO BE REMOVED
import com.efuture.titan.mysql.parse.ParseUtil;
import com.alibaba.cobar.server.parser.ServerParseSelect;

public class SelectProcessor implements CommandProcessor {

  private static final Map<Integer, Class> responseMap = new HashMap<Integer, Class>();
  static {
    responseMap.put(ParseSelect.DATABASE, SelectDatabaseResponse.class);
    responseMap.put(ParseSelect.VERSION_COMMENT, SelectVersionCommentResponse.class);
    responseMap.put(ParseSelect.USER, SelectUserResponse.class);
    responseMap.put(ParseSelect.VERSION, SelectVersionResponse.class);
    responseMap.put(ParseSelect.IDENTITY, SelectIdentityResponse.class);
    responseMap.put(ParseSelect.LAST_INSERT_ID, SelectLastInsertIdResponse.class);
  }

  private MySQLFrontendConnection conn;

  public void init(SessionState ss) {
    this.conn = (MySQLFrontendConnection) ss.getFrontendConnection();
  }

  @Override
  public void run(String sql) {
    int selectOp = ParseSelect.parse(sql);
    if (selectOp == ParseSelect.OTHER) {
      Driver driver = new Driver();
      driver.init(SessionState.get(conn));
      driver.run(sql);
    } else {
      Class responseClass = responseMap.get(selectOp);
      Response response = (Response) ReflectionUtils.newInstance(responseClass);
      response.setFrontendConnection(conn);
      if (selectOp == ParseSelect.IDENTITY) {
        response = selectIdentity(response, sql);
      } else if (selectOp == ParseSelect.LAST_INSERT_ID) {
        response = selectLastInsertId(response, sql);
      }
      response.response();
    }
  }

  // TODO
  private Response selectIdentity(Response response, String sql) {
    SelectIdentityResponse res = (SelectIdentityResponse) response;
    // just use cobar codes

    sql = rebuildSQL(sql);
    int offset = 0;
    for (; offset < sql.length(); ++offset) {
      if (sql.charAt(offset) == '@') {
        break;
      }
    }
    int indexOfAtAt = offset;
    offset = ServerParseSelect.indexAfterIdentity(sql, offset);
    String orgName = sql.substring(indexOfAtAt, offset);
    offset = ServerParseSelect.skipAs(sql, offset);
    String alias = com.alibaba.cobar.parser.util.ParseUtil.parseAlias(sql, offset);
    if (alias == null) {
      alias = orgName;
    }
    res.setFieldName(alias);
    res.setFieldOrgName(orgName);
    //response = res;
    return res;
  }

  private Response selectLastInsertId(Response response, String sql) {
    SelectLastInsertIdResponse res = (SelectLastInsertIdResponse) response;
    // use cobar codes
    sql = rebuildSQL(sql);
    int offset = 0;
    for (; offset < sql.length(); ++offset) {
      if (sql.charAt(offset) == 'l') {
        break;
      }
    }
    offset = ServerParseSelect.indexAfterLastInsertIdFunc(sql, offset);
    offset = ServerParseSelect.skipAs(sql, offset);
    String alias = com.alibaba.cobar.parser.util.ParseUtil.parseAlias(sql, offset);
    res.setFieldName(alias);
    //response = res;
    return res;
  }

  private String rebuildSQL(String sql) {
    sql = ParseUtil.removeComment(sql);
    StringTokenizer st = new StringTokenizer(sql);
    String select = st.nextToken();
    StringBuilder sb = new StringBuilder();
    while (st.hasMoreTokens()) {
      sb.append(st.nextToken());
      sb.append(" ");
    }
    return sb.toString();
  }
}
