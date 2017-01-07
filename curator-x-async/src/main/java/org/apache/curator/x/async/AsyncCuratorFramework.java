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
package org.apache.curator.x.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.api.WatchableAsyncCuratorFramework;
import org.apache.curator.x.async.details.AsyncCuratorFrameworkImpl;

/**
 * Zookeeper framework-style client that returns composable async operations
 * that implement {@link java.util.concurrent.CompletionStage}
 */
public interface AsyncCuratorFramework extends AsyncCuratorFrameworkDsl
{
    /**
     * Takes an old-style Curator instance and returns a new async instance that
     * wraps it. Note: the instance must have been created through a chain that
     * leads back to {@link org.apache.curator.framework.CuratorFrameworkFactory}. i.e.
     * you can have derived instances such as {@link org.apache.curator.framework.WatcherRemoveCuratorFramework}
     * etc. but the original client must have been created by the Factory.
     *
     * @param client instance to wrap
     * @return wrapped instance
     */
    static AsyncCuratorFramework wrap(CuratorFramework client)
    {
        return new AsyncCuratorFrameworkImpl(client);
    }

    /**
     * Returns the client that was originally passed to {@link #wrap(org.apache.curator.framework.CuratorFramework)}
     *
     * @return original client
     */
    CuratorFramework unwrap();

    /**
     * Returns a facade that adds the given UnhandledErrorListener to all background operations
     *
     * @param listener lister to use
     * @return facade
     */
    AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener);
}
