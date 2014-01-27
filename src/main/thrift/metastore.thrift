#!/usr/local/bin/thrift -java

#
# Thrift Service that the MetaStore is built on
#

namespace java com.efuture.titan.metastore

struct Version {
  1: string version,
  2: string comments
}

struct Database {
  1: string name,
  2: string description,
}

struct Table {
  1: string name,
  2: string description,
  3: string dbName,

  4: string tableType,

  # partition
  5: FieldSchema partitionKey,
  6: PartitionRule rule,
  7: list<DataNode> dataNodes,

  # for table join
  8: list<string> childTables,
  9: string parentTable,
  10: FieldSchema parentJoinKey,
  11: FieldSchema joinKey,
}

struct DataNode {
  1: string name,
  2: DataSource source,
}

struct 
