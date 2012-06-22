package com.netflix.curator.framework.recipes.locks;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    private volatile Future<Void> task;

    private static final int        REAPING_THRESHOLD_MS = (int)TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    private static class PathHolder implements Delayed
    {
        private final String path;
        private final long expirationMs;
        private final Mode mode;

        private PathHolder(String path, int delayMs, Mode mode)
        {
            this.path = path;
            this.mode = mode;
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
            long        diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
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
        REAP_INDEFINITELY,
        REAP_UNTIL_DELETE
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
     * @param client client
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, int reapingThresholdMs)
    {
        this(client, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Reaper-%d").build()), reapingThresholdMs);
    }

    /**
     * @param client client
     * @param executor thread pool
     * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
     */
    public Reaper(CuratorFramework client, ExecutorService executor, int reapingThresholdMs)
    {
        this.client = client;
        this.executor = executor;
        this.reapingThresholdMs = reapingThresholdMs;
    }

    /**
     * Add a path (using Mode.REAP_INDEFINITELY) to be checked by the reaper. The path will be checked periodically
     * until the path is removed of the reaper is closed.
     *
     * @param path path to check
     */
    public void     addPath(String path)
    {
        addPath(path, Mode.REAP_INDEFINITELY);
    }

    /**
     * Add a path to be checked by the reaper. The path will be checked periodically
     * until the path is removed of the reaper is closed.
     *
     * @param path path to check
     * @param mode reaping mode
     */
    public void     addPath(String path, Mode mode)
    {
        queue.add(new PathHolder(path, reapingThresholdMs, mode));
    }

    /**
     * Stop reaping the given path
     *
     * @param path path to remove
     * @return true if the path was removed
     */
    public boolean     removePath(String path)
    {
        return queue.remove(new PathHolder(path, reapingThresholdMs, null));
    }

    /**
     * The reaper must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
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
                            PathHolder holder = queue.take();
                            reap(holder.path, holder.mode);
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

    private void reap(String path, Mode mode)
    {
        boolean     addBack = true;
        try
        {
            Stat        stat = client.checkExists().forPath(path);
            if ( stat != null ) // otherwise already deleted
            {
                if ( stat.getNumChildren() == 0 )
                {
                    try
                    {
                        client.delete().forPath(path);
                        log.info("Reaping path: " + path);
                        if ( mode == Mode.REAP_UNTIL_DELETE )
                        {
                            addBack = false;
                        }
                    }
                    catch ( KeeperException.NoNodeException ignore )
                    {
                        // ignore - it must have been deleted by another process/thread
                    }
                    catch ( KeeperException.NotEmptyException ignore )
                    {
                        // ignore - it must have been re-used
                    }
                }
            }
        }
        catch ( Exception e )
        {
            log.error("Trying to reap: " + path, e);
        }

        if ( addBack && !Thread.currentThread().isInterrupted() )
        {
            addPath(path);
        }
    }
}
