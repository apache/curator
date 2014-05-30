#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


if (( $# != 2 )); then
    echo "usage:\ngenerate.sh <path to swift2thrift-generator-cli-N.N.N-standalone.jar> <path to zookeeper-N.N.N.jar>"
    exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$DIR/../../../.." && pwd )"

RPC_PATH="$BASE_DIR/curator-x-rpc/target/classes"

CLASSES=""

for p in services structs exceptions; do
    for f in `ls -m1 $RPC_PATH/org/apache/curator/x/rpc/idl/$p/*.class | xargs -n 1 basename | sed s/\.[^\.]*$//`; do
        if [[ $f != *[\$]* ]]; then
            CLASSES="$CLASSES org.apache.curator.x.rpc.idl.$p.$f";
        fi;
    done;
done;

THRIFT_DIR="$BASE_DIR/curator-x-rpc/src/main/thrift"

PATHS="$1:$2"
PATHS="$PATHS:$BASE_DIR/curator-client/target/classes"
PATHS="$PATHS:$BASE_DIR/curator-framework/target/classes"
PATHS="$PATHS:$BASE_DIR/curator-recipes/target/classes"
PATHS="$PATHS:$RPC_PATH"

java -cp $PATHS com.facebook.swift.generator.swift2thrift.Main \
    -allow_multiple_packages org.apache.curator \
    -namespace cpp org.apache.curator.generated \
    -namespace java org.apache.curator.generated \
    -out "$THRIFT_DIR/curator.thrift" \
    $CLASSES
