
package com.efuture.titan.parse.route;

import com.alibaba.cobar.config.model.SchemaConfig;
import com.alibaba.cobar.route.RouteResultset;
import com.alibaba.cobar.route.ServerRouter;

import com.efuture.titan.common.TitanException;
import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metadata.Meta;
import com.efuture.titan.parse.SemanticAnalyzer;
import com.efuture.titan.session.SessionState;

import com.efuture.titan.util.CobarUtils;

public class Router {

  private Meta meta;

  public Router(SemanticAnalyzer sem) {
    this.meta = sem.getMeta();
  }

  public RoutePlan route(String sql, SessionState ss) throws TitanException {
    RoutePlan plan = new RoutePlan();

    try {
      String db = ss.db;
      SchemaConfig dbConfig = CobarUtils.getSchemaConfigFromMeta(meta, db);

      RouteResultset rrs = ServerRouter.route(dbConfig, sql, ss.charset, null);
      plan.setRouteResultSet(rrs);
      return plan;
    } catch (Exception e) {
      throw new TitanException(e.getMessage());
    }
  }

}
