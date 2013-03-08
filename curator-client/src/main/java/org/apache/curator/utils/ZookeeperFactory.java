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

package org.apache.curator.utils;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public interface ZookeeperFactory
{
    /**
     * Allocate a new ZooKeeper instance
     *
     *
     * @param connectString the connection string
     * @param sessionTimeout session timeout in milliseconds
     * @param watcher optional watcher
     * @param canBeReadOnly if true, allow ZooKeeper client to enter
     *                      read only mode in case of a network partition. See
     *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     *                      for details
     * @return the instance
     * @throws Exception errors
     */
    public ZooKeeper        newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception;
}
