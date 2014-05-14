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

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;

/**
 * Zookeeper framework-style client
 */
public interface CuratorFramework extends Closeable
{
    /**
     * Start the client. Most mutator methods will not work until the client is started
     */
    public void start();

    /**
     * Stop the client
     */
    public void close();

    /**
     * Returns the state of this instance
     *
     * @return state
     */
    public CuratorFrameworkState getState();

    /**
     * Return true if the client is started, not closed, etc.
     *
     * @return true/false
     * @deprecated use {@link #getState()} instead
     */
    public boolean isStarted();

    /**
     * Start a create builder
     *
     * @return builder object
     */
    public CreateBuilder create();

    /**
     * Start a delete builder
     *
     * @return builder object
     */
    public DeleteBuilder delete();

    /**
     * Start an exists builder
     * <p>
     * The builder will return a Stat object as if org.apache.zookeeper.ZooKeeper.exists() were called.  Thus, a null
     * means that it does not exist and an actual Stat object means it does exist.
     *
     * @return builder object
     */
    public ExistsBuilder checkExists();

    /**
     * Start a get data builder
     *
     * @return builder object
     */
    public GetDataBuilder getData();

    /**
     * Start a set data builder
     *
     * @return builder object
     */
    public SetDataBuilder setData();

    /**
     * Start a get children builder
     *
     * @return builder object
     */
    public GetChildrenBuilder getChildren();

    /**
     * Start a get ACL builder
     *
     * @return builder object
     */
    public GetACLBuilder getACL();

    /**
     * Start a set ACL builder
     *
     * @return builder object
     */
    public SetACLBuilder setACL();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    public CuratorTransaction inTransaction();

    /**
     * Perform a sync on the given path - syncs are always in the background
     *
     * @param path                    the path
     * @param backgroundContextObject optional context
     * @deprecated use {@link #sync()} instead
     */
    public void sync(String path, Object backgroundContextObject);

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    public SyncBuilder sync();

    /**
     * Returns the listenable interface for the Connect State
     *
     * @return listenable
     */
    public Listenable<ConnectionStateListener> getConnectionStateListenable();

    /**
     * Returns the listenable interface for events
     *
     * @return listenable
     */
    public Listenable<CuratorListener> getCuratorListenable();

    /**
     * Returns the listenable interface for unhandled errors
     *
     * @return listenable
     */
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable();

    /**
     * Returns a facade of the current instance that does _not_ automatically
     * pre-pend the namespace to all paths
     *
     * @return facade
     * @deprecated use {@link #usingNamespace} passing <code>null</code>
     */
    public CuratorFramework nonNamespaceView();

    /**
     * Returns a facade of the current instance that uses the specified namespace
     * or no namespace if <code>newNamespace</code> is <code>null</code>.
     *
     * @param newNamespace the new namespace or null for none
     * @return facade
     */
    public CuratorFramework usingNamespace(String newNamespace);

    /**
     * Return the current namespace or "" if none
     *
     * @return namespace
     */
    public String getNamespace();

    /**
     * Return the managed zookeeper client
     *
     * @return client
     */
    public CuratorZookeeperClient getZookeeperClient();

    /**
     * Allocates an ensure path instance that is namespace aware
     *
     * @param path path to ensure
     * @return new EnsurePath instance
     */
    public EnsurePath newNamespaceAwareEnsurePath(String path);

    /**
     * Curator can hold internal references to watchers that may inhibit garbage collection.
     * Call this method on watchers you are no longer interested in.
     *
     * @param watcher the watcher
     */
    public void clearWatcherReferences(Watcher watcher);
}
