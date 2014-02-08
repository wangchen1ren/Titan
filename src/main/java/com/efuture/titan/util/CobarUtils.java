
package com.efuture.titan.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.config.model.*;
import com.alibaba.cobar.config.model.rule.*;

import com.efuture.titan.common.TitanException;
import com.efuture.titan.metadata.Meta;
import com.efuture.titan.metastore.*;

public class CobarUtils {

  public static SchemaConfig getSchemaConfigFromMeta(Meta meta,
      String dbName) throws TitanException {
    SchemaConfig res = null;
    Database db = meta.getDatabase(dbName);
    if (db != null) {
      String group = db.getGroup();
      List<String> tableNames = db.getTables();
      Map<String, TableConfig> tables = new HashMap<String, TableConfig>();
      for (String tableName : tableNames) {
        tables.put(tableName, getTableConfigFromMeta(meta, dbName, tableName));
      }
      DataNode node = db.getDataNode();
      String dataNode = "";
      if (node != null) {
        dataNode = node.getName();
      }

      res = new SchemaConfig(dbName, dataNode, group, false, tables);
    }
    return res;
  }

  public static TableConfig getTableConfigFromMeta(Meta meta, String dbName,
      String tableName) throws TitanException {
    TableConfig res = null;
    Table tbl = meta.getTable(dbName, tableName);
    if (tbl != null) {
      List<String> dataNodes = tbl.getDataNodes();
      String dataNode = StringUtils.join(",", dataNodes);

      String ruleName = tbl.getTableRule();
      TableRuleConfig tableRule = null;
      if (ruleName != null) {
        tableRule = getTableRuleConfigFromMeta(meta, ruleName);
      }
      boolean ruleRequired = (ruleName == null) ? false : true;

      res = new TableConfig(tableName, dataNode, tableRule, ruleRequired);
    }
    return res;
  }

  public static TableRuleConfig getTableRuleConfigFromMeta(Meta meta, String ruleName)
      throws TitanException {
    TableRuleConfig res = null;
    TableRule tableRule = meta.getTableRule(ruleName);
    if (tableRule != null) {
      List<Rule> rules = tableRule.getRules();
      List<RuleConfig> ruleList = new ArrayList<RuleConfig>();
      for (Rule rule : rules) {
        String[] columns = (String[]) rule.getColumns().toArray();
        String algorithm = rule.getAlgorithm();
        ruleList.add(new RuleConfig(columns, algorithm));
      }

      res = new TableRuleConfig(ruleName, ruleList);
    }
    return res;
  }
}
