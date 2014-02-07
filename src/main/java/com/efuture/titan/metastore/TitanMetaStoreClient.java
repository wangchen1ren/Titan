
package com.efuture.titan.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;

public class TitanMetaStoreClient implements IMetaStoreClient {
  private static final Log LOG = LogFactory.getLog(TitanMetaStoreClient.class);

  protected TitanConf conf;
  protected ThriftTitanMetaStore.Iface client;
  protected TTransport transport = null;

  protected boolean localMetaStore;
  protected boolean isConnected;

  public TitanMetaStoreClient(TitanConf conf) {
    this.conf = conf;

    String msUri = conf.getVar(ConfVars.TITAN_METASTORE_URI);
    localMetaStore = (msUri == null) ? true : msUri.trim().isEmpty();
    if (localMetaStore) {
      //client = TitanMetaStore.newTMSHandler("titan meta client", conf);
      isConnected = true;
      return;
    }
  }

  public void reconnect() throws MetaException {
  }

  public void close() {
    isConnected = false;
    if (transport != null && transport.isOpen()) {
      transport.close();
    }
  }

  public Database getDatabase(String dbName)
      throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_database(dbName));
  }

  public Table getTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_table(dbName, tableName));
  }

  public TableRule getTableRule(String ruleName)
      throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_table_rule(ruleName));
  }


  private Database deepCopy(Database db) {
    Database copy = null;
    if (db != null) {
      copy = new Database(db);
    }
    return copy;
  }

  private Table deepCopy(Table table) {
    Table copy = null;
    if (table != null) {
      copy = new Table(table);
    }
    return copy;
  }

  private TableRule deepCopy(TableRule rule) {
    TableRule copy = null;
    if (rule != null) {
      copy = new TableRule(rule);
    }
    return copy;
  }
}
