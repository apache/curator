package org.apache.curator.utils;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Decorates an {@link ExecutorService} such that submitted tasks
 * are recorded and can be closed en masse.
 */
public class CloseableScheduledExecutorService extends CloseableExecutorServiceBase
{
    private final ListeningScheduledExecutorService executorService;

    /**
     * @param executorService the service to decorate
     */
    public CloseableScheduledExecutorService(ScheduledExecutorService executorService)
    {
        this.executorService = MoreExecutors.listeningDecorator(executorService);
    }

    @Override
    protected ListeningExecutorService getService()
    {
        return executorService;
    }

    /**
     * Calls {@link ScheduledExecutorService#schedule(Runnable, long, TimeUnit)}, records
     * and returns the future
     *
     * @param command the task to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of
     *         the task and whose <tt>get()</tt> method will return
     *         <tt>null</tt> upon completion
     */
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        return record(executorService.schedule(command, delay, unit));
    }

    /**
     * Calls {@link ScheduledExecutorService#schedule(Callable, long, TimeUnit)}, records
     * and returns the future
     *
     * @param callable the task to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of
     *         the task and whose <tt>get()</tt> method will return
     *         <tt>null</tt> upon completion
     */
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        return record(executorService.schedule(callable, delay, unit));
    }

    /**
     * Calls {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}, records
     * and returns the future
     *
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of
     *         the task, and whose <tt>get()</tt> method will throw an
     *         exception upon cancellation
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        return record(executorService.scheduleAtFixedRate(command, initialDelay, period, unit));
    }

    /**
     * Calls {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}, records
     * and returns the future
     *
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param delay the delay between the termination of one
     * execution and the commencement of the next
     * @param unit the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of
     *         the task, and whose <tt>get()</tt> method will throw an
     *         exception upon cancellation
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        return record(executorService.scheduleWithFixedDelay(command, initialDelay, delay, unit));
    }
}
