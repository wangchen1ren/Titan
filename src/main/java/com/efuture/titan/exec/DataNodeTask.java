
package com.efuture.titan.exec;

public class DataNodeTask {

  private DataNode dataNode;
  private String sql;
  private int replicaIndex;

  public DataNodeTask() {
  }

  public DataNodeTask(DataNode node, String sql) {
    this(node, sql, DEFAULT_REPLICA_INDEX);
  }

  public DataNodeTask(DataNode node, String sql, int replicaIndex) {
    this.dataNode = node;
    this.sql = sql;
    this.replicaIndex = replicaIndex;
  }

  public void setDataNode(DataNode node) {
    this.dataNode = node;
  }

  public DataNode getDataNode() {
    return dataNode;
  }

  public void setSQL(String sql) {
    this.sql = sql;
  }

  public String getSQL() {
    return sql;
  }

  public void setReplicaIndex(int replicaIndex) {
    this.replicaIndex = replicaIndex;
  }

  public void getReplicaIndex() {
    return replicaIndex;
  }

}
