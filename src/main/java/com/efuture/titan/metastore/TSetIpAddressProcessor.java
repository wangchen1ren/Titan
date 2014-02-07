
package com.efuture.titan.metastore;

import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.efuture.titan.metastore.ThriftTitanMetaStore.Iface;

/**
 * TSetIpAddressProcessor passes the IP address of the Thrift client to the TMSHandler.
 */
public class TSetIpAddressProcessor<I extends Iface> extends ThriftTitanMetaStore.Processor<Iface> {

  @SuppressWarnings("unchecked")
  public TSetIpAddressProcessor(I iface) throws SecurityException, NoSuchFieldException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
    InvocationTargetException {
    super(iface);
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setIpAddress(in);

    return super.process(in, out);
  }

  protected void setIpAddress(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (!(transport instanceof TSocket)) {
      return;
    }
    setIpAddress(((TSocket)transport).getSocket());
  }

  protected void setIpAddress(final Socket inSocket) {
    TMSHandler.setIpAddress(inSocket.getInetAddress().toString());
  }
}
