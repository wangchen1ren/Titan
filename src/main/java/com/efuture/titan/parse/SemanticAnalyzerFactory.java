
package com.efuture.titan.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.TitanException;
import com.efuture.titan.common.conf.TitanConf;

public class SemanticAnalyzerFactory {

  public static SemanticAnalyzer get(TitanConf conf, SQLStatement statement) 
      throws TitanException {
    return new SemanticAnalyzer(conf);
  }
}
