
package com.efuture.titan.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metadata.Meta;

public class SemanticAnalyzer {

  private TitanConf conf;

  private Meta meta;

  private String statement;
  

  public SemanticAnalyzer(TitanConf conf) {
    this.conf = conf;
    meta = Meta.get(conf);
  }

  public void analyze(SQLStatement statement) {
    // do nothing
  }

  public Meta getMeta() {
    return meta;
  }
}
