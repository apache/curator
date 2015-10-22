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
package org.apache.curator.framework;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Similar to {@link EnsurePath} but creates containers.
 *
 */
public class EnsureContainers
{
    private final CuratorFramework client;
    private final String path;
    private final AtomicBoolean ensureNeeded = new AtomicBoolean(true);

    /**
     * @param client the client
     * @param path path to ensure is containers
     */
    public EnsureContainers(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = path;
    }

    /**
     * The first time this method is called, all nodes in the
     * path will be created as containers if needed
     *
     * @throws Exception errors
     */
    public void ensure() throws Exception
    {
        if ( ensureNeeded.get() )
        {
            internalEnsure();
        }
    }

    private synchronized void internalEnsure() throws Exception
    {
        if ( ensureNeeded.compareAndSet(true, false) )
        {
            client.createContainers(path);
        }
    }
}
