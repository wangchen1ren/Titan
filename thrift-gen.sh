#!/bin/sh

dir=`dirname $0`
dir=`cd $dir; pwd`

THRIFT_VERSION="0.9.1"

THRIFT_IN_PATH=`which thrift 2>/dev/null`
if [ -f ${THRIFT_IN_PATH} ]; then
  THRIFT_DIR=`dirname "$THRIFT_IN_PATH"`/..
fi
THRIFT_HOME=${THRIFT_HOME:-${THRIFT_PREFIX:-$THRIFT_DIR}}
if [ "$THRIFT_HOME" == "" ]; then
  echo "Cannot find thrift"
  exit 4;
fi
THRIFT=$THRIFT_HOME/bin/thrift

if $THRIFT -version | grep $THRIFT_VERSION >/dev/null 2>&1; then
  GEN_OUT=$dir/src/main/java/gen
  mkdir -p $GEN_OUT

  $THRIFT -gen java -o $GEN_OUT $dir/src/main/thrift/metastore.thrift
else
  echo "Thrift version have to be $THRIFT_VERSION"
  exit 1
fi

