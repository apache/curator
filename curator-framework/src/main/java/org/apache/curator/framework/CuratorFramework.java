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
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.state.ConnectionStateErrorPolicy;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    @Deprecated
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
     * Start a reconfig builder
     *
     * @return builder object
     */
    public ReconfigBuilder reconfig();

    /**
     * Start a getConfig builder
     *
     * @return builder object
     */
    public GetConfigBuilder getConfig();

    /**
     * Start a transaction builder
     *
     * @return builder object
     * @deprecated use {@link #transaction()} instead
     */
    public CuratorTransaction inTransaction();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    public CuratorMultiTransaction transaction();

    /**
     * Allocate an operation that can be used with {@link #transaction()}.
     * NOTE: {@link CuratorOp} instances created by this builder are
     * reusable.
     *
     * @return operation builder
     */
    public TransactionOp transactionOp();

    /**
     * Perform a sync on the given path - syncs are always in the background
     *
     * @param path                    the path
     * @param backgroundContextObject optional context
     * @deprecated use {@link #sync()} instead
     */
    @Deprecated
    public void sync(String path, Object backgroundContextObject);

    /**
     * Create all nodes in the specified path as containers if they don't
     * already exist
     *
     * @param path path to create
     * @throws Exception errors
     */
    public void createContainers(String path) throws Exception;

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    public SyncBuilder sync();

    /**
     * Start a remove watches builder.
     *
     * @return builder object
     * @deprecated use {@link #watchers()} in ZooKeeper 3.6+
     */
    public RemoveWatchesBuilder watches();

    /**
     * Start a watches builder.
     *
     * @return builder object
     */
    public WatchesBuilder watchers();

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
     * @deprecated Since 2.9.0 - use {@link #usingNamespace} passing <code>null</code>
     */
    @Deprecated
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
     * @deprecated Since 2.9.0 - prefer {@link CreateBuilder#creatingParentContainersIfNeeded()}, {@link ExistsBuilder#creatingParentContainersIfNeeded()}
     * or {@link CuratorFramework#createContainers(String)}
     */
    @Deprecated
    public EnsurePath newNamespaceAwareEnsurePath(String path);

    /**
     * Curator can hold internal references to watchers that may inhibit garbage collection.
     * Call this method on watchers you are no longer interested in.
     *
     * @param watcher the watcher
     *
     * @deprecated As of ZooKeeper 3.5 Curators recipes will handle removing watcher references
     * when they are no longer used. If you write your own recipe, follow the example of Curator
     * recipes and use {@link #newWatcherRemoveCuratorFramework} calling {@link WatcherRemoveCuratorFramework#removeWatchers()}
     * when closing your instance.
     */
    @Deprecated
    public void clearWatcherReferences(Watcher watcher);

    /**
     * Block until a connection to ZooKeeper is available or the maxWaitTime has been exceeded
     * @param maxWaitTime The maximum wait time. Specify a value &lt;= 0 to wait indefinitely
     * @param units The time units for the maximum wait time.
     * @return True if connection has been established, false otherwise.
     * @throws InterruptedException If interrupted while waiting
     */
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException;

    /**
     * Block until a connection to ZooKeeper is available. This method will not return until a
     * connection is available or it is interrupted, in which case an InterruptedException will
     * be thrown
     * @throws InterruptedException If interrupted while waiting
     */
    public void blockUntilConnected() throws InterruptedException;

    /**
     * Returns a facade of the current instance that tracks
     * watchers created and allows a one-shot removal of all watchers
     * via {@link WatcherRemoveCuratorFramework#removeWatchers()}
     *
     * @return facade
     */
    public WatcherRemoveCuratorFramework newWatcherRemoveCuratorFramework();

    /**
     * Return the configured error policy
     *
     * @return error policy
     */
    public ConnectionStateErrorPolicy getConnectionStateErrorPolicy();

    /**
     * Current maintains a cached view of the Zookeeper quorum config.
     *
     * @return the current config
     */
    public QuorumVerifier getCurrentConfig();

    /**
     * Return this instance's schema set
     *
     * @return schema set
     */
    SchemaSet getSchemaSet();

    /**
     * Return true if this instance is running in ZK 3.4.x compatibility mode
     *
     * @return true/false
     */
    boolean isZk34CompatibilityMode();

    /**
     * Return true if this instance is running in ZK 3.5.x compatibility mode
     *
     * @return true/false
     */
    boolean isZk35CompatibilityMode();

    /**
     * Calls {@link #notifyAll()} on the given object after first synchronizing on it. This is
     * done from the {@link #runSafe(Runnable)} thread.
     *
     * @param monitorHolder object to sync on and notify
     * @return a CompletableFuture that can be used to monitor when the call is complete
     * @since 4.1.0
     */
    default CompletableFuture<Void> postSafeNotify(Object monitorHolder)
    {
        return runSafe(() -> {
            synchronized(monitorHolder) {
                monitorHolder.notifyAll();
            }
        });
    }

    /**
     * Curator (and user) recipes can use this to run notifyAll
     * and other blocking calls that might normally block ZooKeeper's event thread.

     * @param runnable proc to call from a safe internal thread
     * @return a CompletableFuture that can be used to monitor when the call is complete
     * @since 4.1.0
     */
    CompletableFuture<Void> runSafe(Runnable runnable);
}
