#!/bin/bash
export TSDB_HOME=/root/tsdb/opentsdb-2.2.0
pushd $TSDB_HOME
./build/tsdb tsd --port=8282 --staticroot=build/staticroot --zkquorum=localhost:2181 --cachedir=./cache --zkbasedir=/hbase-unsecure
