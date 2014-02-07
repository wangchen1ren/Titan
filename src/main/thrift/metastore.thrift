#!/usr/local/bin/thrift -java

#
# Thrift Service that the MetaStore is built on
#

namespace java com.efuture.titan.metastore

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name,
  2: string type,
  3: string comment,
}

struct Rule {
  1: list<string> columns,
  2: string algorithm,
}

struct TableRule {
  1: string name,
  2: list<Rule> rules,
}

struct DataNode {
  1: string name,
#  2: DataSource source,
}

struct Table {
  1: string name,
  2: string description,
  3: string dbName,
  4: string tableType,

  # partition
  5: FieldSchema partitionKey,
  7: list<DataNode> dataNodes,

  # for table join
  8: list<string> childTables,
  9: string parentTable,
  10: FieldSchema parentJoinKey,
  11: FieldSchema joinKey,

  12: optional string tableRule,
}

struct Database {
  1: string name,
  2: string description,
  3: string uri,
  4: list<string> tables,
  5: optional string group,
  6: optional DataNode dataNode,
}

exception MetaException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

service ThriftTitanMetaStore {
  Database get_database(1:string name)
      throws(1: NoSuchObjectException o1,
             2: MetaException o2)

  Table get_table(1: string dbName, 2: string tableName)
      throws(1: NoSuchObjectException o1,
             2: MetaException o2)

  TableRule get_table_rule(1: string ruleName)
      throws(1: NoSuchObjectException o1,
             2: MetaException o2)
}
