
package com.efuture.titan.security;

public interface Authenticator {

  public boolean authenticate(byte[] data);

}
