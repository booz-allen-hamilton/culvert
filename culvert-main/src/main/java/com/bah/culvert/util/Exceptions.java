package com.bah.culvert.util;

/**
 * Utility class for interacting with {@link Exception Exceptions}
 */
public final class Exceptions {

  /** to prevent instantiation */
  private Exceptions(){}
  
  /**
   * Wrap the throwable in a RuntimeException, if it isn't one already. If it
   * is, the original exception is returned.
   * @param t to check/wrap.
   * @return a RuntimeException (which can be thrown from any method)
   */
  public static RuntimeException asRuntime(Throwable t) {
    if (t instanceof RuntimeException)
      return (RuntimeException) t;
    return new RuntimeException(t);
  }

}
