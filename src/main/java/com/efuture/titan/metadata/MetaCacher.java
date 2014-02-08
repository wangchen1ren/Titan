
package com.efuture.titan.metadata;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;

public class MetaCacher {

  private TitanConf conf;

  public MetaCacher(TitanConf conf) {
    this.conf = conf;
    initCache();
  }

  public void clear() {
    initCache();
  }

  private void initCache() {
  }
}
