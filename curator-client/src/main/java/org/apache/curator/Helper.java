/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator;

import org.apache.zookeeper.ZooKeeper;

class Helper {
    private final Data data;

    static class Data {
        volatile ZooKeeper zooKeeperHandle = null;
        volatile String connectionString = null;
    }

    Helper(Data data) {
        this.data = data;
    }

    ZooKeeper getZooKeeper() throws Exception {
        return data.zooKeeperHandle;
    }

    String getConnectionString() {
        return data.connectionString;
    }

    int getNegotiatedSessionTimeoutMs() {
        return (data.zooKeeperHandle != null) ? data.zooKeeperHandle.getSessionTimeout() : 0;
    }

    void resetConnectionString(String connectionString) {
        data.connectionString = connectionString;
    }
}
