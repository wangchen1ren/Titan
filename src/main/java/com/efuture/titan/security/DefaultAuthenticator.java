
package com.efuture.titan.security;

public class DefaultAuthenticator implements Authenticator {

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
