
package com.efuture.titan.exec;

import java.util.List;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.parse.SemanticAnalyzer;
import com.efuture.titan.session.SessionState;

public class QueryPlan {

  private List<DataNodeTask> dataNodeTasks;
  private int type;

  private boolean isAutoCommit;

  //private Merger merger;

  public QueryPlan(SessionState ss, SemanticAnalyzer sem) {
  }

  public boolean isAutoCommit() {
    return isAutoCommit;
  }

  public boolean isSingleNodeTask() {
    return dataNodeTasks.size() == 1;
  }

  public List<DataNodeTask> getDataNodeTasks() {
    return dataNodeTasks;
  }


}
