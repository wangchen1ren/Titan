
package com.efuture.titan.mysql.exec;

import java.util.HashMap;
import java.util.Map;

import com.efuture.titan.common.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.OkPacket;
import com.efuture.titan.mysql.parse.MySQLParse;
import com.efuture.titan.mysql.session.MySQLSessionState;

public class QueryProcessor {

  private TitanConf conf;
  private MySQLFrontendConnection conn;

  private static final Map<Integer, Processor> processorMap = new HashMap<Integer, Processor>();

  static {
    processorMap.put(MySQLParse.BEGIN, new UnsupportedProcessor());
    processorMap.put(MySQLParse.COMMIT, new CommitProcessor());
    processorMap.put(MySQLParse.EXPLAIN, new ExplainProcessor());
    processorMap.put(MySQLParse.HELP, new UnsupportedProcessor());
    processorMap.put(MySQLParse.KILL, new KillProcessor());
    processorMap.put(MySQLParse.MYSQL_CMD_COMMENT, new CommentProcessor());
    processorMap.put(MySQLParse.MYSQL_COMMENT, new CommentProcessor());
    processorMap.put(MySQLParse.ROLLBACK, new RollbackProcessor());
    processorMap.put(MySQLParse.SAVEPOINT, new UnsupportedProcessor());
    processorMap.put(MySQLParse.SELECT, new SelectProcessor());
    processorMap.put(MySQLParse.SET, new SetProcessor());
    processorMap.put(MySQLParse.SHOW, new ShowProcessor());
    processorMap.put(MySQLParse.START, new StartProcessor());
    processorMap.put(MySQLParse.USE, new UseProcessor());
  }

  public QueryProcessor(TitanConf conf, MySQLFrontendConnection conn) {
    this.conf = conf;
    this.conn = conn;
  }

  public void query(String sql) {
    int type = MySQLParse.parse(sql);
    Processor processor = processorMap.get(type);
    if (processor != null) {
      processor.process(sql, conn);
    } else {      
      QueryExecutor.execute(sql, type, conn);
    }
  }

}
