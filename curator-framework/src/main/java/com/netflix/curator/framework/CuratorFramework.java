/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.api.CreateBuilder;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.DeleteBuilder;
import com.netflix.curator.framework.api.ExistsBuilder;
import com.netflix.curator.framework.api.GetACLBuilder;
import com.netflix.curator.framework.api.GetChildrenBuilder;
import com.netflix.curator.framework.api.GetDataBuilder;
import com.netflix.curator.framework.api.SetACLBuilder;
import com.netflix.curator.framework.api.SetDataBuilder;
import com.netflix.curator.utils.EnsurePath;
import java.io.Closeable;
import java.util.concurrent.Executor;

/**
 * Zookeeper framework-style client
 */
public interface CuratorFramework extends Closeable
{
    /**
     * Start the client. Most mutator methods will not work until the client is started
     */
    public void     start();

    /**
     * Stop the client
     */
    public void     close();

    /**
     * Return true if the client is started, not closed, etc.
     *
     * @return true/false
     */
    public boolean  isStarted();

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
     * Perform a sync on the given path - syncs are always in the background
     *
     * @param path the path
     * @param backgroundContextObject optional context
     */
    public void     sync(String path, Object backgroundContextObject);

    /**
     * Add an event listener
     *
     * @param listener the listener
     */
    public void     addListener(CuratorListener listener);

    /**
     * Add an event listener
     *
     * @param listener the listener
     * @param executor executor within which to submit events
     */
    public void     addListener(CuratorListener listener, Executor executor);

    /**
     * Remove the given listener
     *
     * @param listener listener to remove
     */
    public void     removeListener(CuratorListener listener);

    /**
     * Returns a facade of the current instance that does _not_ automatically
     * pre-pend the namespace to all paths
     *
     * @return facade
     */
    public CuratorFramework nonNamespaceView();

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
    public EnsurePath    newNamespaceAwareEnsurePath(String path);
}
