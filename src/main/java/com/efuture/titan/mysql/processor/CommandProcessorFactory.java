
package com.efuture.titan.mysql.processor;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.config.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.CommandProcessor;
import com.efuture.titan.exec.Driver;
import com.efuture.titan.mysql.net.MySQLFrontendConnection;
import com.efuture.titan.mysql.net.packet.OkPacket;
import com.efuture.titan.mysql.parse.MySQLParse;
import com.efuture.titan.mysql.session.MySQLSessionState;
import com.efuture.titan.util.ReflectionUtils;

public class CommandProcessorFactory {
  private static final Map<Integer, Class> processorMap =
      new HashMap<Integer, Class>();
  static {
    processorMap.put(MySQLParse.SELECT, SelectProcessor.class);
    /*
    processorMap.put(MySQLParse.BEGIN, new UnsupportedProcessor());
    processorMap.put(MySQLParse.COMMIT, new CommitProcessor());
    processorMap.put(MySQLParse.EXPLAIN, new ExplainProcessor());
    processorMap.put(MySQLParse.HELP, new UnsupportedProcessor());
    processorMap.put(MySQLParse.KILL, new KillProcessor());
    processorMap.put(MySQLParse.MYSQL_CMD_COMMENT, new CommentProcessor());
    processorMap.put(MySQLParse.MYSQL_COMMENT, new CommentProcessor());
    processorMap.put(MySQLParse.ROLLBACK, new RollbackProcessor());
    processorMap.put(MySQLParse.SAVEPOINT, new UnsupportedProcessor());
    processorMap.put(MySQLParse.SET, new SetProcessor());
    processorMap.put(MySQLParse.SHOW, new ShowProcessor());
    processorMap.put(MySQLParse.START, new StartProcessor());
    processorMap.put(MySQLParse.USE, new UseProcessor());
    */
  }

  private CommandProcessorFactory() {}

  public static CommandProcessor get(MySQLSessionState ss, String sql) {
    int type = MySQLParse.parse(sql);
    Class cls = processorMap.get(type);
    if (cls != null) {
      CommandProcessor processor = (CommandProcessor) ReflectionUtils.newInstance(cls);
      processor.init(ss);
      return processor;
    } else {      
      // Driver
      Driver driver = new Driver(ss);
      return driver;
    }
  }

}
