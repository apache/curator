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

package org.apache.curator.ensemble;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.Closeable;
import java.io.IOException;

/**
 * Abstraction that provides the ZooKeeper connection string
 */
public interface EnsembleProvider extends Closeable
{
    /**
     * Curator will call this method when {@link CuratorZookeeperClient#start()} is
     * called
     * 
     * @throws Exception errors
     */
    public void         start() throws Exception;

    /**
     * Return the current connection string to use. Curator will call this each
     * time it needs to create a ZooKeeper instance
     * 
     * @return connection string (per {@link ZooKeeper#ZooKeeper(String, int, Watcher)} etc.)
     */
    public String       getConnectionString();

    /**
     * Curator will call this method when {@link CuratorZookeeperClient#close()} is called
     * 
     * @throws IOException errors
     */
    public void         close() throws IOException;
}
