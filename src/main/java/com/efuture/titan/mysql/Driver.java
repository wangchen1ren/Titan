
package com.efuture.titan.mysql;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.processor.CommandProcessor;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.mysql.parse.SemanticAnalyzer;
import com.efuture.titan.mysql.parse.SemanticAnalyzerFactory;
import com.efuture.titan.route.Router;
import com.efuture.titan.route.RoutePlan;
import com.efuture.titan.util.StringUtils;

public class Driver implements CommandProcessor {
  private static final Log LOG = LogFactory.getLog(Driver.class);

  private TitanConf conf;
  private MySQLSessionState ss;
  private MySQLFrontendConnection conn;

  private RoutePlan plan;

  public Driver() {
  }

  public Driver(MySQLSessionState ss) {
    init(ss);
  }

  public void init(MySQLSessionState ss) {
    this.ss = ss;
    this.conf = ss.getConf();
  }

  public void run(String sql) {
    // pre-run-hook

    int ret = compile(sql);

    // lock
    ret = execute();

    // unlock
    
    // post-run-hook
  }

  public int compile(String sql) {
    //stmt = removeSchema(stmt, schema.getName());
    //RouteResultset rrs = new RouteResultset(stmt, sqlType);

    // pre-compile-hook

    LOG.info("Start Query: '" + sql + "'");
    // ast
    try {
      SQLStatementParser parser = new MySqlStatementParser(sql);
      List<SQLStatement> stmtList = parser.parseStatementList();
      SQLStatement statement = stmtList.get(0);

      LOG.info("Parse Completed");

      /*
         StringBuilder out = new StringBuilder();
         MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);

         for (SQLStatement stmt : stmtList) {
         stmt.accept(visitor);
         out.append(";");
         }   
         LOG.info("statement: " + out.toString());
         */

      // Semantic Analyse
      SemanticAnalyzer sem = SemanticAnalyzerFactory.get(ss.getConf(), statement);
      sem.analyze(statement);

      LOG.info("Semantic Analysis Completed");

    } catch (ParserException e) {
      LOG.error("Parse error for '" + sql + "', with exception:" +
          StringUtils.stringifyException(e));
    }

    if (conf.getBoolVar(ConfVars.TITAN_SERVER_AUTHORIZATION_ENABLED)) {
      doAuthorization();
    }


    Router router = new Router();
    plan = router.route(sql, ss);

    // post-compile-hook

    return 0;
  }

  public int execute() {
    return 0;
  }

  public void doAuthorization() {
  }

}
