/**
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
import org.apache.zookeeper.admin.ZooKeeperAdmin;

public class DefaultZookeeperFactory implements ZookeeperFactory
{
    // hide org.apache.zookeeper.admin.ZooKeeperAdmin in a nested class so that Curator continues to work with ZK 3.4.x
    private static class ZooKeeperAdminMaker implements ZookeeperFactory
    {
        static final ZooKeeperAdminMaker instance = new ZooKeeperAdminMaker();

        @Override
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
        {
            return new ZooKeeperAdmin(connectString, sessionTimeout, watcher, canBeReadOnly);
        }
    }

    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
    {
        if ( Compatibility.hasZooKeeperAdmin() )
        {
            return ZooKeeperAdminMaker.instance.newZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
        }
        return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
    }
}
