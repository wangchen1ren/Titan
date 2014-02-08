
package com.efuture.titan.exec;

import com.efuture.titan.session.SessionState;

public interface CommandProcessor {

  public void init(SessionState ss);

  public void run(String sql);

}
