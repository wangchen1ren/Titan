
package com.efuture.titan.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.conf.TitanConf;

public class SemanticAnalyzerFactory {

  public static SemanticAnalyzer get(TitanConf conf, SQLStatement statement) {
    return new SemanticAnalyzer(conf);
  }
}
