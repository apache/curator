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

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.Watcher;

class FailedRemoveWatchManager extends FailedOperationManager<FailedRemoveWatchManager.FailedRemoveWatchDetails>
{
    FailedRemoveWatchManager(CuratorFramework client)
    {
        super(client);
    }

    @Override
    protected void executeGuaranteedOperationInBackground(FailedRemoveWatchDetails details)
            throws Exception
    {
        if(details.watcher == null)
        {
            client.watches().removeAll().guaranteed().inBackground().forPath(details.path);
        }
        else
        {
            client.watches().remove(details.watcher).guaranteed().inBackground().forPath(details.path);
        }
    }
    
    static class FailedRemoveWatchDetails
    {
        public final String path;
        public final Watcher watcher;
        
        public FailedRemoveWatchDetails(String path, Watcher watcher)
        {
            this.path = path;
            this.watcher = watcher;
        }
    }
}
