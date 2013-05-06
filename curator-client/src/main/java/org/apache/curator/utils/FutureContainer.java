package org.apache.curator.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

public class FutureContainer implements Closeable
{
    private final List<Future<?>> futures = Lists.newArrayList();
    private final ExecutorService executorService;

    private class QueueingFuture<T> extends FutureTask<T>
    {
        private final RunnableFuture<T> task;

        QueueingFuture(RunnableFuture<T> task)
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

    public FutureContainer(ExecutorService executorService)
    {
        this.executorService = executorService;
    }

    @VisibleForTesting
    int size()
    {
        return futures.size();
    }

    @Override
    public void close()
    {
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
     */
    public<V> void submit(Callable<V> task)
    {
        FutureTask<V> futureTask = new FutureTask<V>(task);
        executorService.execute(new QueueingFuture<V>(futureTask));
    }

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task.  Upon completion, this task may be
     * taken or polled.
     *
     * @param task the task to submit
     */
    public void submit(Runnable task)
    {
        FutureTask<Void> futureTask = new FutureTask<Void>(task, null);
        executorService.execute(new QueueingFuture<Void>(futureTask));
    }
}
