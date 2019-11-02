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
package org.apache.curator.v2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.WatchesBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkV2Impl;

public interface CuratorFrameworkV2 extends CuratorFramework
{
    /**
     * Wrap a CuratorFramework instance to gain access to newer ZooKeeper features
     *
     * @param client client instance
     * @return wrapped client
     */
    static CuratorFrameworkV2 wrap(CuratorFramework client)
    {
        return new CuratorFrameworkV2Impl(client);
    }

    /**
     * Start a watches builder
     *
     * @return builder
     */
    WatchesBuilder watches();

    @Override
    CuratorFrameworkV2 usingNamespace(String newNamespace);

    @Override
    WatcherRemoveCuratorFrameworkV2 newWatcherRemoveCuratorFramework();
}
