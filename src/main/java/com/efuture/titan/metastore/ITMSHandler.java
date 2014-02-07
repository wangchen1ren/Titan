
package com.efuture.titan.metastore;

import com.efuture.titan.common.conf.TitanConf;

public interface ITMSHandler extends ThriftTitanMetaStore.Iface {

  public void setConf(TitanConf conf);

}
