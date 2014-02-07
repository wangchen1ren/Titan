
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

  public static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ReflectionUtils.class.getClassLoader();
    }   
    return classLoader;
  }

  public static Class<?> getClass(String className) {
    try {
      return Class.forName(className, true, getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(className + " class not found");
    }
  }

  /**
   * Create an object of the given class.
   * @param theClass
   * @param parameterTypes
   *          an array of parameterTypes for the constructor
   * @param initargs
   *          the list of arguments for the constructor
   */
  public static <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes,
      Object[] initargs) {
    // Perform some sanity checks on the arguments.
    if (parameterTypes.length != initargs.length) {
      throw new IllegalArgumentException(
          "Number of constructor parameter types doesn't match number of arguments");
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> clazz = parameterTypes[i];
      if (!(clazz.isInstance(initargs[i]))) {
        throw new IllegalArgumentException("Object : " + initargs[i]
            + " is not an instance of " + clazz);
      }
    }

    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(parameterTypes);
      meth.setAccessible(true);
      return meth.newInstance(initargs);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }


}
