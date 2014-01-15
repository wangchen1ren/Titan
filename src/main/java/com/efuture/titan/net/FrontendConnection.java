
package com.efuture.titan.net;

import java.nio.channels.SocketChannel;
import java.util.List;

import com.efuture.titan.common.ErrorCode;

public class FrontendConnection extends BaseConnection {

  public FrontendConnection(SocketChannel channel,
      List<NIOHandler> handlers) {
    super(channel, handlers);
  }

  @Override
  public void handle(final byte[] data) {
    // 异步处理前端数据
    processor.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (NIOHandler handler : handlers) {
            handler.handle(data);
          }
        } catch (Throwable t) {
          error(ErrorCode.ERR_HANDLE_DATA, t);
        }
      }
    });
  }

}
