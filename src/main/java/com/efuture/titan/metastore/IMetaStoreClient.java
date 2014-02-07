
package com.efuture.titan.metastore;

import org.apache.thrift.TException;

public interface IMetaStoreClient {

  public void reconnect() throws MetaException;

  public void close();

  public Database getDatabase(String dbName)
      throws NoSuchObjectException, MetaException, TException;

  public Table getTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException;

  public TableRule getTableRule(String ruleName)
      throws NoSuchObjectException, MetaException, TException;
}
