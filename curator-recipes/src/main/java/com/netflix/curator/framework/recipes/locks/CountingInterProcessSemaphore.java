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

package com.netflix.curator.framework.recipes.locks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A traditional-style counting semaphore that is distributed. Essentially, this is a lock on a shared counter.
 * When the counter is greater than 0, calls to {@code acquire()} succeed. When the counter is 0,
 * calls to {@code acquire()} block until {@code release()} is called.
 */
public class CountingInterProcessSemaphore
{
    private final CuratorFramework  client;
    private final String            lockPath;
    private final InterProcessMutex lock;
    private final Watcher           watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            notifyFromWatcher();
        }
    };

    /**
     * @param client client
     * @param lockPath ZK path to use for locking and holding the count
     */
    public CountingInterProcessSemaphore(CuratorFramework client, String lockPath)
    {
        this.client = client;
        this.lockPath = lockPath;
        lock = new InterProcessMutex(client, lockPath);
    }

    @VisibleForTesting
    synchronized int getCurrentLeases() throws Exception
    {
        lock.acquire();
        try
        {
            return getCurrentAvailable(false);
        }
        finally
        {
            lock.release();
        }
    }

    /**
     * Acquire 1 lease, reducing the number of available leases by 1. If there are no leases
     * currently available, this method blocks until another thread/process calls {@code release()}.
     *
     * @throws Exception ZK errors
     */
    public synchronized void acquire() throws Exception
    {
        acquire(1, 0, null);
    }

    /**
     * Acquire {@code qty} leases, reducing the number of available leases by {@code qty}. If there are not enough leases
     * currently available, this method blocks until another thread/process releases enough leases.
     *
     * @param qty number of leases to acquire
     * @throws Exception ZK errors
     */
    public synchronized void acquire(int qty) throws Exception
    {
        acquire(qty, 0, null);
    }

    /**
     * Acquire 1 lease, reducing the number of available leases by 1. If there are no leases
     * currently available, this method blocks until another thread/process calls {@code release()}
     * or until the specified timeout elapses.
     *
     * @param time amount of time to block until leases are available
     * @param unit time unit
     * @throws Exception ZK errors
     * @return true if the lease was acquired, false if not
     */
    public synchronized boolean acquire(long time, TimeUnit unit) throws Exception
    {
        return acquire(1, time, unit);
    }

    /**
     * Acquire {@code qty} leases, reducing the number of available leases by {@code qty}. If there are not enough leases
     * currently available, this method blocks until another thread/process releases enough leases
     * or until the specified timeout elapses.
     *
     * @param qty number of leases to acquire
     * @param time amount of time to block until leases are available
     * @param unit time unit
     * @throws Exception ZK errors
     * @return true if the lease was acquired, false if not
     */
    public synchronized boolean acquire(int qty, long time, TimeUnit unit) throws Exception
    {
        Preconditions.checkArgument(qty > 0);

        boolean           hasTimeout = (unit != null);
        final long        startTicks = System.currentTimeMillis();
        final long        maxMilliseconds = hasTimeout ? TimeUnit.MILLISECONDS.convert(time, unit) : Long.MAX_VALUE;

        for(;;)
        {
            lock.acquire();
            try
            {
                int currentAvailable = getCurrentAvailable(false);
                if ( currentAvailable >= qty )
                {
                    setLeaseCount(currentAvailable - qty);
                    break;
                }

                getCurrentAvailable(true);
            }
            finally
            {
                lock.release();
            }

            if ( hasTimeout )
            {
                long        elapsed = (System.currentTimeMillis() - startTicks);
                long        millisecondsToWait = maxMilliseconds - elapsed;
                if ( millisecondsToWait <= 0 )
                {
                    return false;
                }
                wait(millisecondsToWait);
            }
            else
            {
                wait();
            }
        }

        return true;
    }

    /**
     * <p>Releases a lease, increasing the number of available leases by
     * one.  If any threads/processes are trying to acquire a lease, then one is
     * selected and given the lease that was just released.</p>
     *
     * <p>There is no requirement that a thread that releases a lease must
     * have acquired that lease by calling {@link #acquire}.
     * Correct usage of a semaphore is established by programming convention
     * in the application.</p>
     *
     * @throws Exception ZK errors
     */
    public synchronized void  release() throws Exception
    {
        release(1);
    }

    /**
     * <p>Releases the given number of leases, increasing the number of
     * available leases by that amount.
     * If any threads/processes are trying to acquire leases, then one
     * is selected and given the leases that were just released.
     * If the number of available leases satisfies its request
     * then it is unblocked otherwise it will block until sufficient leases are available.
     * If there are still leases available
     * after this thread/process has been satisfied, then those leases
     * are assigned in turn to other threads trying to acquire leases.</p>
     *
     * <p>There is no requirement that a thread that releases a lease must
     * have acquired that lease by calling {@link #acquire acquire}.
     * Correct usage of a semaphore is established by programming convention
     * in the application.</p>
     * 
     * @param qty number of leases to release
     * @throws Exception ZK errors
     */
    public synchronized void  release(int qty) throws Exception
    {
        Preconditions.checkArgument(qty > 0);

        lock.acquire();
        try
        {
            int     current = getCurrentAvailable(false);
            int     newQty = current + qty;
            setLeaseCount(newQty);
        }
        finally
        {
            lock.release();
        }
    }

    private void    setLeaseCount(int count) throws Exception
    {
        Preconditions.checkArgument(count >= 0);

        byte[]      bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(count);
        client.setData().forPath(lockPath, bytes);
    }

    private int     getCurrentAvailable(boolean watched) throws Exception
    {
        GetDataBuilder  builder = client.getData();
        if ( watched )
        {
            builder.usingWatcher(watcher);
        }
        byte[]      bytes = builder.forPath(lockPath);
        if ( bytes.length == 0 )
        {
            return 0;
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    private synchronized void notifyFromWatcher()
    {
        notifyAll();
    }
}
