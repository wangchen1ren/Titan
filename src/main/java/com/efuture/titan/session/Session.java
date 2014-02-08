
package com.efuture.titan.session;

import com.efuture.titan.net.FrontendConnection;

public interface Session {

  FrontendConnection getFrontendConnection();

  void execute();

  void commit();

  void rollback();

  void cancel();

  void terminate();

}
