
package com.efuture.titan.metastore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;

public class MetaContent {
  private static final Log LOG = LogFactory.getLog(MetaContent.class);

  private static AtomicBoolean initialized = new AtomicBoolean(false);
  private static Map<String, Database> dbMap = new HashMap<String, Database>();
  private static Map<String, Map<String, Table>> tableMap = new HashMap<String, Map<String, Table>>();
  private static Map<String, TableRule> tableRuleMap = new HashMap<String, TableRule>();

  private TitanConf conf;

  public MetaContent(TitanConf conf) {
    this.conf = conf;
    initMeta();
  }

  public void initMeta() {
    if (!initialized.get()) {
      String loadMode = conf.getVar(ConfVars.TITAN_METASTORE_LOAD_MODE).toUpperCase();
      if (loadMode.equals("XML")) {
        loadFromXML();
      } else if (loadMode.equals("DB")) {
        loadFromDB();
      } else {
        // default
        loadFromXML();
      }
    }
  }

  public Map<String, Database> getDatabases() {
    return dbMap;
  }

  public Map<String, Map<String, Table>> getTables() {
    return tableMap;
  }

  public Map<String, TableRule> getTableRules() {
    return tableRuleMap;
  }

  private void loadFromXML() {
    // TODO
  }

  private void loadFromDB() {
    throw new UnsupportedOperationException("Load meta from db not support.");
  }
}
