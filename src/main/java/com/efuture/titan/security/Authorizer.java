
package com.efuture.titan.security;

public interface Authorizer {

  // global
  public void authorize(String user)
      throws AuthorizationException;

  // db
  public void authorize(String user, String dbName)
      throws AuthorizationException;

  public void authorize(String user, String dbName, String tableName)
      throws AuthorizationException;

}
