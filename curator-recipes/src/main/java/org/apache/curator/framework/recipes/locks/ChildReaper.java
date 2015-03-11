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
package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableScheduledExecutorService;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.utils.PathUtils;

/**
 * Utility to reap empty child nodes of a parent node. Periodically calls getChildren on
 * the node and adds empty nodes to an internally managed {@link Reaper}
 */
public class ChildReaper implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Reaper reaper;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final CuratorFramework client;
    private final Collection<String> paths = Sets.newConcurrentHashSet();
    private final Reaper.Mode mode;
    private final CloseableScheduledExecutorService executor;
    private final int reapingThresholdMs;
    private final LeaderLatch leaderLatch;
    private final Set<String> lockSchema;

    private volatile Future<?> task;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param mode reaping mode
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode)
    {
        this(client, path, mode, newExecutorService(), Reaper.DEFAULT_REAPING_THRESHOLD_MS, null);
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, int reapingThresholdMs)
    {
        this(client, path, mode, newExecutorService(), reapingThresholdMs, null);
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param executor executor to use for background tasks
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs)
    {
        this(client, path, mode, executor, reapingThresholdMs, null);
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param executor executor to use for background tasks
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     * @param leaderPath if not null, uses a leader selection so that only 1 reaper is active in the cluster
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs, String leaderPath)
    {
        this(client, path, mode, executor, reapingThresholdMs, leaderPath, Collections.<String>emptySet());
    }


    /**
     * @param client the client
     * @param path path to reap children from
     * @param executor executor to use for background tasks
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     * @param leaderPath if not null, uses a leader selection so that only 1 reaper is active in the cluster
     * @param lockSchema a set of the possible subnodes of the children of path that must be reaped in addition to the child nodes
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs, String leaderPath, Set<String> lockSchema)
    {
        this.client = client;
        this.mode = mode;
        this.executor = new CloseableScheduledExecutorService(executor);
        this.reapingThresholdMs = reapingThresholdMs;
        if (leaderPath != null)
        {
            leaderLatch = new LeaderLatch(client, leaderPath);
        }
        else
        {
            leaderLatch = null;
        }
        this.reaper = new Reaper(client, executor, reapingThresholdMs, leaderLatch);
        this.lockSchema = lockSchema;
        addPath(path);
    }

    /**
     * The reaper must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        task = executor.scheduleWithFixedDelay
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    doWork();
                }
            },
            reapingThresholdMs,
            reapingThresholdMs,
            TimeUnit.MILLISECONDS
        );
        if (leaderLatch != null)
        {
            leaderLatch.start();
        }
        reaper.start();
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            CloseableUtils.closeQuietly(reaper);
            if (leaderLatch != null)
            {
                CloseableUtils.closeQuietly(leaderLatch);
            }
            task.cancel(true);
        }
    }

    /**
     * Add a path to reap children from
     *
     * @param path the path
     * @return this for chaining
     */
    public ChildReaper addPath(String path)
    {
        paths.add(PathUtils.validatePath(path));
        return this;
    }

    /**
     * Remove a path from reaping
     *
     * @param path the path
     * @return true if the path existed and was removed
     */
    public boolean removePath(String path)
    {
        return paths.remove(PathUtils.validatePath(path));
    }

    public static ScheduledExecutorService newExecutorService()
    {
        return ThreadUtils.newFixedThreadScheduledPool(2, "ChildReaper");
    }

    private void doWork()
    {
        if (shouldDoWork())
        {
            for ( String path : paths )
            {
                try
                {
                    List<String> children = client.getChildren().forPath(path);
                    for ( String name : children )
                    {
                        String childPath = ZKPaths.makePath(path, name);
                        addPathToReaperIfEmpty(childPath);
                        for ( String subNode : lockSchema )
                        {
                            addPathToReaperIfEmpty(ZKPaths.makePath(childPath, subNode));
                        }

                    }
                }
                catch ( Exception e )
                {
                    log.error("Could not get children for path: " + path, e);
                }
            }
        }
    }

    private void addPathToReaperIfEmpty(String path) throws Exception
    {
        Stat stat = client.checkExists().forPath(path);
        if ( (stat != null) && (stat.getNumChildren() == 0) )
        {
            reaper.addPath(path, mode);
        }
    }

    private boolean shouldDoWork()
    {
        return this.leaderLatch == null || this.leaderLatch.hasLeadership();
    }
}
