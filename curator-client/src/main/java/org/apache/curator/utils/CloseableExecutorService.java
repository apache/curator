package org.apache.curator.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Decoration on an ExecutorService that tracks created futures and provides
 * a method to close futures created via this class
 */
public class CloseableExecutorService implements Closeable
{
    private final Set<Future<?>> futures = Sets.newSetFromMap(Maps.<Future<?>, Boolean>newConcurrentMap());
    private final ExecutorService executorService;
    protected final AtomicBoolean isOpen = new AtomicBoolean(true);

    protected class InternalFutureTask<T> extends FutureTask<T>
    {
        private final RunnableFuture<T> task;

        InternalFutureTask(RunnableFuture<T> task)
        {
            super(task, null);
            this.task = task;
            futures.add(task);
        }

        protected void done()
        {
            futures.remove(task);
        }
    }

    /**
     * @param executorService the service to decorate
     */
    public CloseableExecutorService(ExecutorService executorService)
    {
        this.executorService = executorService;
    }

    /**
     * Returns <tt>true</tt> if this executor has been shut down.
     *
     * @return <tt>true</tt> if this executor has been shut down
     */
    public boolean isShutdown()
    {
        return !isOpen.get();
    }

    @VisibleForTesting
    int size()
    {
        return futures.size();
    }

    /**
     * Closes any tasks currently in progress
     */
    @Override
    public void close()
    {
        isOpen.set(false);
        Iterator<Future<?>> iterator = futures.iterator();
        while ( iterator.hasNext() )
        {
            Future<?> future = iterator.next();
            iterator.remove();
            if ( !future.cancel(true) )
            {
                System.err.println("Could not cancel");
                throw new RuntimeException("Could not cancel");
            }
        }
    }

    /**
     * Submits a value-returning task for execution and returns a Future
     * representing the pending results of the task.  Upon completion,
     * this task may be taken or polled.
     *
     * @param task the task to submit
     * @return a future to watch the task
     */
    public<V> Future<V> submit(Callable<V> task)
    {
        Preconditions.checkState(isOpen.get(), "CloseableExecutorService is closed");

        InternalFutureTask<V> futureTask = new InternalFutureTask<V>(new FutureTask<V>(task));
        executorService.execute(futureTask);
        return futureTask;
    }

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task.  Upon completion, this task may be
     * taken or polled.
     *
     * @param task the task to submit
     * @return a future to watch the task
     */
    public Future<?> submit(Runnable task)
    {
        Preconditions.checkState(isOpen.get(), "CloseableExecutorService is closed");

        InternalFutureTask<Void> futureTask = new InternalFutureTask<Void>(new FutureTask<Void>(task, null));
        executorService.execute(futureTask);
        return futureTask;
    }
}
