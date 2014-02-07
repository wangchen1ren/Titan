
package com.efuture.titan.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;

public class TMSHandler implements ITMSHandler {

  private String name;
  private TitanConf conf;

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
  }

  public void setConf(TitanConf conf) {
    this.conf = conf;
  }

  public Database get_database(String dbName)
      throws NoSuchObjectException, MetaException {
    return null;
  }

  public Table get_table(String dbName, String tableName)
      throws NoSuchObjectException, MetaException {
    return null;
  }

  public TableRule get_table_rule(String ruleName)
      throws NoSuchObjectException, MetaException {
    return null;
  }
}

