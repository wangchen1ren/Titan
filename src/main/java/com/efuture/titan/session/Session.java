
package com.efuture.titan.session;

import com.efuture.titan.net.FrontendConnection;

public interface Session {

  SessionState getSessionState();

  void execute();

  void commit();

  void rollback();

  void cancel();

  void terminate();

  void close();

}
