
package com.efuture.titan.security;

public interface Authenticator {

  public void authenticate(String user, byte[] password)
      throws AuthenticationException;

  public String getUser();

}
