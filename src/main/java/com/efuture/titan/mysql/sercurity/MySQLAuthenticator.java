
package com.efuture.titan.mysql.security;

import com.efuture.titan.security.Authenticator;

public class MySQLAuthenticator implements Authenticator {

  public boolean authenticate(byte[] data) {
    return true;
  }
}
