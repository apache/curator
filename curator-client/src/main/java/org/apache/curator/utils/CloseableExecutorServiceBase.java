package org.apache.curator.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Decorates an {@link ExecutorService} such that submitted tasks
 * are recorded and can be closed en masse.
 */
abstract class CloseableExecutorServiceBase implements Closeable
{
    private final Set<Future<?>> futures = Sets.newSetFromMap(Maps.<Future<?>, Boolean>newConcurrentMap());
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected abstract ListeningExecutorService getService();

    @Override
    public void close()
    {
        isClosed.set(true);
        Iterator<Future<?>> iterator = futures.iterator();
        while ( iterator.hasNext() )
        {
            Future<?> future = iterator.next();
            iterator.remove();
            future.cancel(true);
        }
    }

    /**
     * @see ExecutorService#isShutdown()
     * @return true/false
     */
    public boolean isShutdown()
    {
        return getService().isShutdown();
    }

    /**
     * @see ExecutorService#isTerminated()
     * @return true/false
     */
    public boolean isTerminated()
    {
        return getService().isTerminated();
    }

    /**
     * Calls {@link ExecutorService#submit(Callable)}, records
     * and returns the future
     *
     * @param task task to submit
     * @return the future
     */
    public <T> Future<T> submit(Callable<T> task)
    {
        return record(getService().submit(task));
    }

    /**
     * Calls {@link ExecutorService#submit(Runnable)}, records
     * and returns the future
     *
     * @param task task to submit
     * @return the future
     */
    public Future<?> submit(Runnable task)
    {
        return record(getService().submit(task));
    }

    @VisibleForTesting
    int size()
    {
        return futures.size();
    }

    protected <T> ScheduledFuture<T> record(final ScheduledFuture<T> future)
    {
        if ( isClosed.get() )
        {
            future.cancel(true);
        }
        else
        {
            futures.add(future);
        }
        return future;
    }

    protected <T> Future<T> record(final ListenableFuture<T> future)
    {
        Runnable listener = new Runnable()
        {
            @Override
            public void run()
            {
                futures.remove(future);
            }
        };
        if ( isClosed.get() )
        {
            future.cancel(true);
        }
        else
        {
            futures.add(future);
            future.addListener(listener, MoreExecutors.sameThreadExecutor());
        }
        return future;
    }
}
