
package com.efuture.titan.util;

import com.alibaba.cobar.config.model.*;
import com.alibaba.cobar.config.model.rule.*;

import com.efuture.titan.metadata.Meta;

import com.efuture.titan.metastore.*;

public class CobarUtils {

  public static SchemaConfig getSchemaConfigFromMeta(Meta meta, String dbName) {
    Database db = meta.getDatabase(dbName);
    String group = db.getGroup();
    List<String> tableNames = db.getTables();
    Map<String, TableConfig> tables = new HashMap<String, TableConfig>();
    for (String t : tableNames) {
      tables.put(t, getTableConfigFromMeta(meta, db, t));
    }
    DataNode node = db.getDataNode();
    String dataNode = "";
    if (node != null) {
      dataNode = node.getName();
    }

    SchemaConfig res = new SchemaConfig(dbName, dataNode, group, false, tables);
    return res;
  }

  public static TableConfig getTableConfigFromMeta(Meta meta, String dbName, String tableName) {
    Table tbl = meta.getTable(dbName, tableName);
    List<DataNode> dataNodes = tbl.getDataNodes();
    List<String> dataNodeNames = new ArrayList<String>();
    for (DataNode dataNode : dataNodes) {
      dataNodeNames.add(dataNode.getName());
    }
    String dataNode = StringUtils.join(',', dataNodeNames);

    String ruleName = tbl.getTableRule();
    TableRuleConfig tableRule = null;
    if (ruleName != null) {
      tableRule = getTableRuleConfigFromMeta(meta, ruleName);
    }
    boolean ruleRequired = rule == null ? false : true;

    TableConfig res = new TableConfig(tableName, dataNode, tableRule, ruleRequired);
    return res;
  }

  public static TableRuleConfig getTableRuleConfigFromMeta(Meta meta, String ruleName) {
    TableRule tableRule = meta.getTableRule(ruleName);
    List<Rule> rules = tableRule.getRules();
    List<RuleConfig> ruleList = new ArrayList<RuleConfig>();
    for (Rule rule : rules) {
      String[] columns = rule.getColumns().toArray();
      String algorithm = rule.getAlgorithm();
      ruleList.add(new RuleConfig(columns, algorithm));
    }
    
    TableRuleConfig res = new TableRuleConfig(name, ruleList);
    return res;
  }
}
