
package com.efuture.titan.metadata;

import java.util.HashMap;
import java.util.Map;

public class DbMeta {

  private String name;
  private Map<String, TableMeta> tables;

  private boolean sharding;
  private String dataNode;

  /** 
   * key is join relation ,A.ID=B.PARENT_ID value is Root Table ,if a->b*->c*
   * ,then A is root table
   */
  private final Map<String, TableMeta> joinRel2TableMap = new HashMap<String, TableMeta>();

}
