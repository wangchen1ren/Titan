#!/usr/local/bin/thrift -java

#
# Thrift Service that the MetaStore is built on
#

namespace java com.efuture.titan.metastore

struct Version {
  1: string version,
  2: string comments
}

