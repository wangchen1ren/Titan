
package com.efuture.titan.metastore;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.util.ReflectionUtils;

/**
 * RetryingMetaStoreClient. Creates a proxy for a IMetaStoreClient
 * implementation and retries calls to it on failure.
 * If the login user is authenticated using keytab, it relogins user before
 * each call.
 *
 */
public class RetryingMetaStoreClient implements InvocationHandler {
  private static final Log LOG = LogFactory.getLog(RetryingMetaStoreClient.class.getName());

  private final IMetaStoreClient base;
  private final int retryLimit;
  private final int retryInterval;

  protected RetryingMetaStoreClient(TitanConf conf, TitanMetaHookLoader hookLoader,
      Class<? extends IMetaStoreClient> msClientClass) throws MetaException {
    this.retryLimit = conf.getIntVar(ConfVars.TITAN_METASTORE_RETRIES);
    this.retryInterval = conf.getIntVar(ConfVars.TITAN_METASTORE_RETRY_INTERVAL);

    this.base = (IMetaStoreClient) ReflectionUtils.newInstance(msClientClass, new Class[] {
      TitanConf.class, TitanMetaHookLoader.class}, new Object[] {conf, hookLoader});
  }

  public static IMetaStoreClient getProxy(TitanConf conf, TitanMetaHookLoader hookLoader,
      String mscClassName) throws MetaException {

    Class<? extends IMetaStoreClient> baseClass = (Class<? extends IMetaStoreClient>)
      ReflectionUtils.getClass(mscClassName);

    RetryingMetaStoreClient handler = new RetryingMetaStoreClient(conf, hookLoader, baseClass);

    return (IMetaStoreClient) Proxy.newProxyInstance(RetryingMetaStoreClient.class.getClassLoader(),
        baseClass.getInterfaces(), handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object ret = null;
    int retriesMade = 0;
    TException caughtException = null;
    while (true) {
      try {
        if(retriesMade > 0){
          base.reconnect();
        }
        ret = method.invoke(base, args);
        break;
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      } catch (InvocationTargetException e) {
        if ((e.getCause() instanceof TApplicationException) ||
            (e.getCause() instanceof TProtocolException) ||
            (e.getCause() instanceof TTransportException)) {
          caughtException = (TException) e.getCause();
        } else if ((e.getCause() instanceof MetaException) &&
            e.getCause().getMessage().matches("JDO[a-zA-Z]*Exception")) {
          caughtException = (MetaException) e.getCause();
        } else {
          throw e.getCause();
        }
      }

      if (retriesMade >=  retryLimit) {
        throw caughtException;
      }
      retriesMade++;
      LOG.warn("MetaStoreClient lost connection. Attempting to reconnect.",
          caughtException);

      Thread.sleep(retryInterval * 1000);
    }
    return ret;
  }

}
