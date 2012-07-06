package com.netflix.curator.framework.recipes.locks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Utility to delete parent paths of locks, etc. Periodically checks paths added to the reaper.
 * If at check time, there are no children, the path is deleted.
 */
public class Reaper implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final ExecutorService executor;
    private final int reapingThresholdMs;
    private final DelayQueue<PathHolder> queue = new DelayQueue<PathHolder>();
    private final Set<String> activePaths = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private volatile Future<Void> task;

    private static final int REAPING_THRESHOLD_MS = (int)TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    @VisibleForTesting
    static final int EMPTY_COUNT_THRESHOLD = 3;

    private static class PathHolder implements Delayed
    {
        private final String path;
        private final long expirationMs;
        private final Mode mode;
        private final int emptyCount;

        private PathHolder(String path, int delayMs, Mode mode, int emptyCount)
        {
            this.path = path;
            this.mode = mode;
            this.emptyCount = emptyCount;
            this.expirationMs = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(expirationMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o)
        {
            long diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            PathHolder that = (PathHolder)o;

            if ( path != null ? !path.equals(that.path) : that.path != null )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return path.hashCode();
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
        this(client, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Reaper-%d").build()), REAPING_THRESHOLD_MS);
    }

    /**
     * Uses the given reaping threshold and creates an internal thread pool
     *
     * @param client             client
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, int reapingThresholdMs)
    {
        this(client, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Reaper-%d").build()), reapingThresholdMs);
    }

    /**
     * @param client             client
     * @param executor           thread pool
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, ExecutorService executor, int reapingThresholdMs)
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
        queue.add(new PathHolder(path, reapingThresholdMs, mode, 0));
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
                            while ( !Thread.currentThread().isInterrupted() && (state.get() == State.STARTED) )
                            {
                                PathHolder holder = queue.take();
                                reap(holder);
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
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            try
            {
                queue.clear();
                task.cancel(true);
            }
            catch ( Exception e )
            {
                log.error("Canceling task", e);
            }
        }
    }

    @VisibleForTesting
    int getEmptyCount(final String path)
    {
        PathHolder found = Iterables.find
            (
                queue,
                new Predicate<PathHolder>()
                {
                    @Override
                    public boolean apply(PathHolder holder)
                    {
                        return holder.path.equals(path);
                    }
                },
                null
            );

        return (found != null) ? found.emptyCount : -1;
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
            queue.add(new PathHolder(holder.path, reapingThresholdMs, holder.mode, newEmptyCount));
        }
    }
}
