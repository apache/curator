/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ThreadUtils;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

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
    private final String path;
    private final Reaper.Mode mode;
    private final ExecutorService executor;
    private final int reapingThresholdMs;

    private volatile Future<Void> task;

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
        this(client, path, mode, newExecutorService(), Reaper.DEFAULT_REAPING_THRESHOLD_MS);
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, int reapingThresholdMs)
    {
        this(client, path, mode, newExecutorService(), reapingThresholdMs);
    }

    /**
     * @param client the client
     * @param path path to reap children from
     * @param executor executor to use for background tasks
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     * @param mode reaping mode
     */
    public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ExecutorService executor, int reapingThresholdMs)
    {
        this.client = client;
        this.path = path;
        this.mode = mode;
        this.executor = executor;
        this.reapingThresholdMs = reapingThresholdMs;
        this.reaper = new Reaper(client, executor, reapingThresholdMs);
    }

    /**
     * The reaper must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        task = executor.submit
        (
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        while ( !Thread.currentThread().isInterrupted() )
                        {
                            Thread.sleep(reapingThresholdMs);
                            doWork();
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
            }
        );

        reaper.start();
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            Closeables.closeQuietly(reaper);
            task.cancel(true);
        }
    }

    private static ExecutorService newExecutorService()
    {
        return ThreadUtils.newFixedThreadPool(2, "ChildReaper");
    }

    private void doWork()
    {
        try
        {
            List<String>        children = client.getChildren().forPath(path);
            for ( String name : children )
            {
                String  thisPath = ZKPaths.makePath(path, name);
                Stat    stat = client.checkExists().forPath(thisPath);
                if ( (stat != null) && (stat.getNumChildren() == 0) )
                {
                    reaper.addPath(thisPath, mode);
                }
            }
        }
        catch ( Exception e )
        {
            log.error("Could not get children for path: " + path, e);
        }
    }
}
