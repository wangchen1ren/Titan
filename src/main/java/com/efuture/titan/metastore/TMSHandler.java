
package com.efuture.titan.metastore;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;

public class TMSHandler implements ITMSHandler {

  private String name;
  private TitanConf conf;
  private MetaContent content;

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
    this.content = new MetaContent(conf);
  }

  @Override
  public void setConf(TitanConf conf) {
    this.conf = conf;
  }

  @Override
  public Database get_database(String dbName)
      throws NoSuchObjectException, MetaException {
    try {
      Database res = content.getDatabases().get(dbName);
      if (res == null) {
        throw new NoSuchObjectException("Database " + dbName + " not found");
      }
      return res;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public Table get_table(String dbName, String tableName)
      throws NoSuchObjectException, MetaException {
    try {
      Table res = null;
      Map<String, Table> tables = content.getTables().get(dbName);
      if (tables != null) {
        res = tables.get(tableName);
      }
      if (res == null) {
        throw new NoSuchObjectException("Table " + dbName + "." + tableName + " not found");
      }
      return res;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public TableRule get_table_rule(String ruleName)
      throws NoSuchObjectException, MetaException {
    try {
      TableRule res = content.getTableRules().get(ruleName);
      if (res == null) {
        throw new NoSuchObjectException("TableRule " + ruleName + " not found");
      }
      return res;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }
}

