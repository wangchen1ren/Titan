
package com.efuture.titan.route;

import com.alibaba.cobar.config.model.SchemaConfig;
import com.alibaba.cobar.route.RouteResultset;
import com.alibaba.cobar.route.ServerRouter;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.metadata.Meta;
import com.efutrue.titan.parse.SemanticAnalyzer;
import com.efuture.titan.session.SessionState;

import com.efuture.titan.util.CobarUtils;

public class Router {

  private Meta meta;

  public Router(SemanticAnalyzer sem) {
    this.meta = sem.getMeta();
  }

  public RoutePlan route(String sql, SessionState ss) throws Exception {
    RoutePlan plan = new RoutePlan();

    String db = ss.db;
    SchemaConfig dbConfig = CobarUtils.getSchemaConfigFromMeta(meta, db);

    RouteResultset rrs = ServerRouter.route(dbConfig, sql, ss.charset, null);
    plan.setRouteResultSet(rrs);
    return plan;
  }

}
