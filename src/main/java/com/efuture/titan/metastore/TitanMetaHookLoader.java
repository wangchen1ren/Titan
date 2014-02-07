
package com.efuture.titan.metastore;

public interface TitanMetaHookLoader {

  public TitanMetaHook getHook(Table tbl) throws MetaException;

}
