
package com.efuture.titan.net;

import java.nio.channels.SocketChannel;

import com.alibaba.cobar.config.ErrorCode;
import com.efuture.titan.common.conf.TitanConf;

public abstract class FrontendConnection extends AbstractConnection {

  public FrontendConnection(TitanConf conf, SocketChannel channel) {
    super(conf, channel);
  }

  abstract public void startSession();

  abstract public void closeSession();

  @Override
  public void handle(final byte[] data) {
    final FrontendConnection obj = this;
    // 异步处理前端数据
    processor.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          if (handler != null) {
            handler.handle(obj, data);
          }
        } catch (Throwable t) {
          error(ErrorCode.ERR_HANDLE_DATA, t);
        }
      }
    });
  }

  public void writeErrMessage(int errno, String msg) {
    writeErrMessage((byte) 1, errno, msg);
  }

  abstract public void writeErrMessage(byte id, int errno, String msg);
}
