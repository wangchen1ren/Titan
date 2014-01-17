package com.efuture.titan.net;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.net.NIOConnection;

public abstract class NIOHandler {

  protected TitanConf conf;

  public NIOHandler(TitanConf conf) {
    this.conf = conf;
  }

  abstract public void handle(NIOConnection conn, byte[] data);

}
