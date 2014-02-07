
package com.efuture.titan.metastore;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;

public class TMSHandler implements ITMSHandler {

  private String name;
  private TitanConf conf;

  private Map<String, Database> dbMap;
  private Map<String, Map<String, Table>> tableMap;
  private Map<String, TableRule> tableRuleMap;

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }    
  };   

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }    

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }    

  public TMSHandler(String name, TitanConf conf) {
    this.name = name;
    this.conf = conf;

    initMeta();
  }

  public void setConf(TitanConf conf) {
    this.conf = conf;
  }

  public void initMeta() {
    dbMap = new HashMap<String, Database>();
    tableMap = new HashMap<String, Map<String, Table>>();
    tableRuleMap = new HashMap<String, TableRule>();
  }

  public Database get_database(String dbName)
      throws NoSuchObjectException, MetaException {
    return dbMap.get(dbName);
  }

  public Table get_table(String dbName, String tableName)
      throws NoSuchObjectException, MetaException {
    Table res = null;
    Map<String, Table> tables = tableMap.get(dbName);
    if (tables != null) {
      res = tables.get(tableName);
    }
    return res;
  }

  public TableRule get_table_rule(String ruleName)
      throws NoSuchObjectException, MetaException {
    return tableRuleMap.get(ruleName);
  }
}

