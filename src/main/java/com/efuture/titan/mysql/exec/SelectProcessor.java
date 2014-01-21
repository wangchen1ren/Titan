
package com.efuture.titan.mysql.exec;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.parse.MySQLParse;
import com.efuture.titan.mysql.parse.ParseSelect;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.mysql.response.Response;
import com.efuture.titan.mysql.response.SelectDatabaseResponse;
import com.efuture.titan.mysql.response.SelectIdentityResponse;
import com.efuture.titan.mysql.response.SelectLastInsertIdResponse;
import com.efuture.titan.mysql.response.SelectUserResponse;
import com.efuture.titan.mysql.response.SelectVersionResponse;
import com.efuture.titan.mysql.response.SelectVersionCommentResponse;
import com.efuture.titan.mysql.response.SessionIncrementResponse;
import com.efuture.titan.mysql.response.SessionIsolationResponse;
import com.efuture.titan.util.ReflectionUtils;

// TO BE REMOVED
import com.efuture.titan.mysql.parse.ParseUtil;
import org.opencloudb.server.parser.ServerParseSelect;

public class SelectProcessor implements Processor {

  private static final Map<Integer, Class> responseMap = new HashMap<Integer, Class>();
  static {
    responseMap.put(ParseSelect.DATABASE, SelectDatabaseResponse.class);
    responseMap.put(ParseSelect.VERSION_COMMENT, SelectVersionCommentResponse.class);
    responseMap.put(ParseSelect.USER, SelectUserResponse.class);
    responseMap.put(ParseSelect.VERSION, SelectVersionResponse.class);
    responseMap.put(ParseSelect.SESSION_INCREMENT, SessionIncrementResponse.class);
    responseMap.put(ParseSelect.SESSION_ISOLATION, SessionIsolationResponse.class);
    responseMap.put(ParseSelect.IDENTITY, SelectIdentityResponse.class);
    responseMap.put(ParseSelect.LAST_INSERT_ID, SelectLastInsertIdResponse.class);
  }

  @Override
  public void process(String sql, MySQLFrontendConnection conn) {
    int selectOp = ParseSelect.parse(sql);
    if (selectOp == ParseSelect.OTHER) {
      QueryExecutor.execute(sql, MySQLParse.SELECT, conn);
    } else {
      Class responseClass = responseMap.get(selectOp);
      Response response = (Response) ReflectionUtils.newInstance(responseClass);
      response.setFrontendConnection(conn);
      if (selectOp == ParseSelect.IDENTITY) {
        SelectIdentityResponse res = (SelectIdentityResponse) response;
        // just use cobar codes
        // TODO

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
        String alias = org.opencloudb.parser.util.ParseUtil.parseAlias(sql, offset);
        if (alias == null) {
          alias = orgName;
        }
        res.setFieldName(alias);
        res.setFieldOrgName(orgName);
        response = res;
      } else if (selectOp == ParseSelect.LAST_INSERT_ID) {
        SelectLastInsertIdResponse res = (SelectLastInsertIdResponse) response;
        // use cobar codes
        // TODO
        sql = rebuildSQL(sql);
        int offset = 0;
        for (; offset < sql.length(); ++offset) {
          if (sql.charAt(offset) == 'l') {
            break;
          }
        }
        offset = ServerParseSelect.indexAfterLastInsertIdFunc(sql, offset);
        offset = ServerParseSelect.skipAs(sql, offset);
        String alias = org.opencloudb.parser.util.ParseUtil.parseAlias(sql, offset);
        res.setFieldName(alias);
        response = res;
      }
      response.response();
    }
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