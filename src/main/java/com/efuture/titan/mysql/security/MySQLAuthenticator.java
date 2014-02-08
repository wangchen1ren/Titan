
package com.efuture.titan.mysql.security;

import com.efuture.titan.security.Authenticator;
import com.efuture.titan.security.AuthenticationException;

public class MySQLAuthenticator implements Authenticator {

  private String user;

  @Override
  public void authenticate(String user, byte[] password)
      throws AuthenticationException {
    // pass
    this.user = user;
  }

  @Override
  public String getUser() {
    return user;
  }
}
