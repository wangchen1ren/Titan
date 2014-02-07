
package com.efuture.titan.common;

public class TitanException extends Exception {

  public TitanException() {
    super();
  }

  public TitanException(String message) {
    super(message);
  }

  public TitanException(Throwable cause) {
    super(cause);
  }

  public TitanException(String message, Throwable cause) {
    super(message, cause);
  }
}
