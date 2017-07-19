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
package org.apache.curator.framework.imps;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;

// keep reference to AsyncCallback.Create2Callback in separate class for ZK 3.4 compatibility
class CreateZK35
{
    static void create(ZooKeeper zooKeeper, String path, byte data[], List<ACL> acl, CreateMode createMode, final CompatibleCreateCallback compatibleCallback, Object ctx, long ttl)
    {
        AsyncCallback.Create2Callback callback = new AsyncCallback.Create2Callback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx, String name, Stat stat)
            {
                compatibleCallback.processResult(rc, path, ctx, name, stat);
            }
        };
        zooKeeper.create(path, data, acl, createMode, callback, ctx, ttl);
    }

    private CreateZK35()
    {
    }
}
