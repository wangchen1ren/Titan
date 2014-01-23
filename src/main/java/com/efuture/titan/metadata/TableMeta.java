
package com.efuture.titan.metadata;

import java.util.List;

public class TableMeta {
  public static final int TYPE_GLOBAL_DEFAULT = 0;
  public static final int TYPE_GLOBAL_TABLE = 1;

  private String name;
  private int tableType;
  private List<String> dataNodes;
  private String partitionColumn;
}
