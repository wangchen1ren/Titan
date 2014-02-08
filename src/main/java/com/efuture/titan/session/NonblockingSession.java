
package com.efuture.titan.session;

import com.efuture.titan.net.FrontendConnection;

public class NonblockingSession implements Session {

  @Override
  public FrontendConnection getFrontendConnection() {
    return null;
  }

  @Override
  public void execute() {
  }

  @Override
  public void commit() {
  }

  @Override
  public void rollback() {
  }

  @Override
  public void cancel() {
  }

  @Override
  public void terminate() {
  }

}
