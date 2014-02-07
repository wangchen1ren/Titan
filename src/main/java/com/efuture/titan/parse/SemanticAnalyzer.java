
package com.efuture.titan.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.TitanException;
import com.efuture.titan.metadata.Meta;

public class SemanticAnalyzer {

  private TitanConf conf;
  private Meta meta;

  public SemanticAnalyzer(TitanConf conf) throws TitanException {
    this.conf = conf;
    try {
      meta = Meta.get(conf);
    } catch (Exception e) {
      throw new TitanException(e.getMessage());
    }
  }

  public void analyze(SQLStatement statement) {
    // do nothing
  }

  public Meta getMeta() {
    return meta;
  }
}
