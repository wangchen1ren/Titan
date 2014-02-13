
package com.efuture.titan.parse;

import com.alibaba.druid.sql.ast.SQLStatement;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.TitanException;
import com.efuture.titan.metadata.Meta;
import com.efuture.titan.parse.route.RoutePlan;
import com.efuture.titan.parse.route.Router;

public class SemanticAnalyzer {

  private TitanConf conf;
  private Meta meta;

  private RoutePlan routePlan;

  public SemanticAnalyzer(TitanConf conf) throws TitanException {
    this.conf = conf;
    try {
      meta = Meta.get(conf);
    } catch (Exception e) {
      throw new TitanException(e.getMessage());
    }
  }

  public Meta getMeta() {
    return meta;
  }

  public void analyze(SQLStatement statement) {
    // route
    //Router router = new Router(this);
  }

}
