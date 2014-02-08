
package com.efuture.titan.security;

public interface Authorizer {

  public void authorize() throws AuthorizationException;

}
