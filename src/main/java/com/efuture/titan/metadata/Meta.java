
package com.efuture.titan.metadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metastore.*;
import com.efuture.titan.metastore.IMetaStoreClient;
import com.efuture.titan.metastore.MetaException;

public class Meta {
  private static final Log LOG = LogFactory.getLog(Meta.class);

  private static Meta meta;

  private TitanConf conf;
  private IMetaStoreClient metaStoreClient;

  public static Meta get(TitanConf conf) throws MetaException {
    boolean needRefresh = false;
    if (meta != null) {
    }
    return get(conf, needRefresh);
  }

  public static Meta get(TitanConf conf, boolean needRefresh) throws MetaException {
    if (meta == null || needRefresh) {
      meta.close();
      meta = new Meta(conf);
      return meta;
    }
    return meta;
  }

  public Meta(TitanConf conf) {
    this.conf = conf;
  }

  public void close() {
  }

  private IMetaStoreClient getMSC() throws MetaException {
    if (metaStoreClient == null) {
      metaStoreClient = createMetaStoreClient();
    }
    return metaStoreClient;
  }

  public Database getDatabase(String dbName) {
    return null;
  }

  public Table getTable(String dbName, String tableName) {
    return null;
  }

  public TableRule getTableRule(String ruleName) {
    return null;
  }

}
