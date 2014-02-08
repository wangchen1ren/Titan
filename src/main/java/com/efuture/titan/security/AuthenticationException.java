
package com.efuture.titan.security;

public class AuthenticationException extends Exception {

  public AuthenticationException() {
    super();
  }

  public AuthenticationException(String message) {
    super(message);
  }

  public AuthenticationException(Throwable cause) {
    super(cause);
  }

  public AuthenticationException(String message, Throwable cause) {
    super(message, cause);
  }

}
