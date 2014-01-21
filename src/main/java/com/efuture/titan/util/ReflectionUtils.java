
package com.efuture.titan.util;

import java.lang.reflect.Constructor;
import java.io.*;
import java.lang.management.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * General reflection utils
 */

public class ReflectionUtils {

  private static final Class[] emptyArray = new Class[]{};
  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
    new ConcurrentHashMap<Class<?>, Constructor<?>>();

  /** Create an object for the given class and initialize it with uri
   *
   * @param theClass class of which an object is created
   * @return a new object
   */
  //@SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(emptyArray);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

}
