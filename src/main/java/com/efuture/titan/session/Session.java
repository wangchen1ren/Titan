
package com.efuture.titan.session;

import com.efuture.titan.exec.QueryPlan;

public interface Session {

  SessionState getSessionState();

  void execute(QueryPlan plan);

  void commit();

  void rollback();

  void cancel();

  void terminate();

  void close();

}
