
package com.efuture.titan.exec;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import com.efuture.titan.common.TitanException;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.exec.blocking.BlockingExecutor;
import com.efuture.titan.net.FrontendConnection;
import com.efuture.titan.parse.SemanticAnalyzer;
import com.efuture.titan.parse.SemanticAnalyzerFactory;
import com.efuture.titan.security.Authorizer;
import com.efuture.titan.session.SessionState;
import com.efuture.titan.util.StringUtils;

public class Driver implements CommandProcessor {
  private static final Log LOG = LogFactory.getLog(Driver.class);

  private TitanConf conf;
  private SessionState ss;

  private QueryPlan plan;

  public Driver() {
  }

  public Driver(SessionState ss) {
    init(ss);
  }

  public void init(SessionState ss) {
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
    // pre-compile-hook
    LOG.info("Start Query: '" + sql + "'");

    try {
      // ast
      SQLStatementParser parser = new MySqlStatementParser(sql);
      List<SQLStatement> stmtList = parser.parseStatementList();
      SQLStatement statement = stmtList.get(0);

      LOG.info("Parse Completed");

      // Semantic Analyse
      SemanticAnalyzer sem = SemanticAnalyzerFactory.get(ss.getConf(), statement);
      sem.analyze(statement);

      LOG.info("Semantic Analysis Completed");

      // Authorization
      if (conf.getBoolVar(ConfVars.TITAN_SERVER_AUTHORIZATION_ENABLED)) {
        doAuthorization(sem);
      }

      plan = new QueryPlan(ss, sem);
    } catch (ParserException e) {
      LOG.error("Parse error for '" + sql + "', with exception:" +
          StringUtils.stringifyException(e));
    } catch (TitanException e) {
      LOG.error("Exception: ", e);
    }
   
    // post-compile-hook

    return 0;
  }

  public void doAuthorization(SemanticAnalyzer sem) {
    Authorizer authorizer = ss.getAuthorizer();
    // get input & output from sem

    // pass
  }

  public int execute() {
    Executor executor = ss.getExecutor();
    if (session == null) {
      executor = new Executor(conf, ss);
      ss.setExecutor(executor);
    }

    executor.execute(plan);
    return 0;
  }

}
