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


if (( $# == 0 )); then
    echo -e "usage:\n\tapply-thrift.sh <language code> <optional target directory>"
    exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$DIR/../../../.." && pwd )"

if (( $# == 2 )); then
    TARGET_DIR="$2"
else
    TARGET_DIR="$BASE_DIR/curator-x-rpc/src/test/java"
fi

thrift -gen $1 -out "$TARGET_DIR" "$BASE_DIR/curator-x-rpc/src/main/thrift/curator.thrift"
