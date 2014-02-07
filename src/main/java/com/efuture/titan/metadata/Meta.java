
package com.efuture.titan.metadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.TitanException;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metastore.*;
import com.efuture.titan.metastore.IMetaStoreClient;
import com.efuture.titan.metastore.MetaException;
import com.efuture.titan.util.StringUtils;

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

  private IMetaStoreClient createMetaStoreClient() {
    return null;
  }

  public Database getDatabase(String dbName) throws TitanException {
    try {
      return getMSC().getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      LOG.warn("Failed to get db meta(" + dbName + ")." +
          StringUtils.stringifyException(e));
      throw new TitanException(e);
    }
  }

  public Table getTable(String dbName, String tableName) throws TitanException {
    try {
      return getMSC().getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      LOG.warn("Failed to get table meta(" + dbName + "." + tableName + ")." +
          StringUtils.stringifyException(e));
      throw new TitanException(e);
    }
  }

  public TableRule getTableRule(String ruleName) throws TitanException {
    try {
      return getMSC().getTableRule(ruleName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      LOG.warn("Failed to get table rule meta(" + ruleName + ")." +
          StringUtils.stringifyException(e));
      throw new TitanException(e);
    }
  }

}
