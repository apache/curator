/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility to clean up parent lock nodes so that they don't stay around as garbage
 */
public class Reaper implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final ScheduledExecutorService executor;
    private final int reapingThresholdMs;
    private final Set<String> activePaths = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    static final int DEFAULT_REAPING_THRESHOLD_MS = (int)TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    @VisibleForTesting
    static final int EMPTY_COUNT_THRESHOLD = 3;

    @VisibleForTesting
    class PathHolder implements Runnable
    {
        final String path;
        final Mode mode;
        final int emptyCount;

        @Override
        public void run()
        {
            reap(this);
        }

        private PathHolder(String path, Mode mode, int emptyCount)
        {
            this.path = path;
            this.mode = mode;
            this.emptyCount = emptyCount;
        }
    }

    public enum Mode
    {
        /**
         * Reap forever, or until removePath is called for the path
         */
        REAP_INDEFINITELY,

        /**
         * Reap until the Reaper succeeds in deleting the path
         */
        REAP_UNTIL_DELETE,

        /**
         * Reap until the path no longer exists
         */
        REAP_UNTIL_GONE
    }

    /**
     * Uses the default reaping threshold of 5 minutes and creates an internal thread pool
     *
     * @param client client
     */
    public Reaper(CuratorFramework client)
    {
        this(client, newExecutorService(), DEFAULT_REAPING_THRESHOLD_MS);
    }

    /**
     * Uses the given reaping threshold and creates an internal thread pool
     *
     * @param client             client
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, int reapingThresholdMs)
    {
        this(client, newExecutorService(), reapingThresholdMs);
    }

    /**
     * @param client             client
     * @param executor           thread pool
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, ScheduledExecutorService executor, int reapingThresholdMs)
    {
        this.client = client;
        this.executor = executor;
        this.reapingThresholdMs = reapingThresholdMs / EMPTY_COUNT_THRESHOLD;
    }

    /**
     * Add a path (using Mode.REAP_INDEFINITELY) to be checked by the reaper. The path will be checked periodically
     * until the reaper is closed.
     *
     * @param path path to check
     */
    public void addPath(String path)
    {
        addPath(path, Mode.REAP_INDEFINITELY);
    }

    /**
     * Add a path to be checked by the reaper. The path will be checked periodically
     * until the reaper is closed, or until the point specified by the Mode
     *
     * @param path path to check
     * @param mode reaping mode
     */
    public void addPath(String path, Mode mode)
    {
        activePaths.add(path);
        executor.schedule(new PathHolder(path, mode, 0), reapingThresholdMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop reaping the given path
     *
     * @param path path to remove
     * @return true if the path was removed
     */
    public boolean removePath(String path)
    {
        return activePaths.remove(path);
    }

    /**
     * The reaper must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            try
            {
                executor.shutdownNow();
            }
            catch ( Exception e )
            {
                log.error("Canceling task", e);
            }
        }
    }

    private void reap(PathHolder holder)
    {
        if ( !activePaths.contains(holder.path) )
        {
            return;
        }

        boolean addBack = true;
        int newEmptyCount = 0;
        try
        {
            Stat stat = client.checkExists().forPath(holder.path);
            if ( stat != null ) // otherwise already deleted
            {
                if ( stat.getNumChildren() == 0 )
                {
                    if ( (holder.emptyCount + 1) >= EMPTY_COUNT_THRESHOLD )
                    {
                        try
                        {
                            client.delete().forPath(holder.path);
                            log.info("Reaping path: " + holder.path);
                            if ( holder.mode == Mode.REAP_UNTIL_DELETE || holder.mode == Mode.REAP_UNTIL_GONE )
                            {
                                addBack = false;
                            }
                        }
                        catch ( KeeperException.NoNodeException ignore )
                        {
                            // Node must have been deleted by another process/thread
                            if ( holder.mode == Mode.REAP_UNTIL_GONE )
                            {
                                addBack = false;
                            }
                        }
                        catch ( KeeperException.NotEmptyException ignore )
                        {
                            // ignore - it must have been re-used
                        }
                    }
                    else
                    {
                        newEmptyCount = holder.emptyCount + 1;
                    }
                }
            }
            else
            {
                if ( holder.mode == Mode.REAP_UNTIL_GONE )
                {
                    addBack = false;
                }
            }
        }
        catch ( Exception e )
        {
            log.error("Trying to reap: " + holder.path, e);
        }

        if ( !addBack )
        {
            activePaths.remove(holder.path);
        }
        else if ( !Thread.currentThread().isInterrupted() && (state.get() == State.STARTED) && activePaths.contains(holder.path) )
        {
            executor.schedule(new PathHolder(holder.path, holder.mode, newEmptyCount), reapingThresholdMs, TimeUnit.MILLISECONDS);
        }
    }

    private static ScheduledExecutorService newExecutorService()
    {
        return ThreadUtils.newSingleThreadScheduledExecutor("Reaper");
    }
}
