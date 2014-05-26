#!/bin/bash

if (( $# != 1 )); then
    echo "missing argument: path to swift2thrift-generator-cli-N.N.N-standalone.jar"
    exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$DIR/../../../.." && pwd )"

RPC_PATH="$BASE_DIR/curator-x-rpc/target/classes"
CLASSES=""
for f in `ls -m1 $RPC_PATH/org/apache/curator/x/rpc/idl/*.class | xargs -n 1 basename | sed s/\.[^\.]*$//`; do CLASSES="$CLASSES $f"; done;

THRIFT_DIR="$BASE_DIR/curator-x-rpc/src/main/thrift"

PATHS="$1"
PATHS="$PATHS:$BASE_DIR/curator-client/target/classes"
PATHS="$PATHS:$BASE_DIR/curator-framework/target/classes"
PATHS="$PATHS:$BASE_DIR/curator-recipes/target/classes"
PATHS="$PATHS:$RPC_PATH"

PACKAGE="org.apache.curator.x.rpc.idl"

java -cp $PATHS com.facebook.swift.generator.swift2thrift.Main -namespace cpp org.apache.curator -out "$THRIFT_DIR/curator.thrift" -package $PACKAGE $CLASSES
