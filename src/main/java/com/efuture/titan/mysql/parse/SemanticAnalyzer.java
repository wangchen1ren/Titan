
package com.efuture.titan.mysql.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metadata.Meta;

public class SemanticAnalyzer {

  private TitanConf conf;

  private Meta meta;
  

  public SemanticAnalyzer(TitanConf conf) {
    this.conf = conf;
  }

  public void analyze(SQLStatement statement) {
  }
}
