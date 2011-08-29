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

package com.netflix.curator.framework.recipes.atomic;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * <p>A counter that attempts atomic increments. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various increment methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the increment methods are guaranteed to succeed.</p>
 */
public abstract class GenericDistributedAtomic<T extends Number & Comparable<T>> implements AtomicNumber<T>
{
    private final CuratorFramework  client;
    private final String            counterPath;
    private final RetryPolicy       retryPolicy;
    private final PromotedToLock    promotedToLock;
    private final InterProcessMutex mutex;

    /**
     * Creates the counter in optimistic mode only - i.e. the promotion to a mutex is not done
     *
     * @param client the client
     * @param counterPath path to hold the counter value
     * @param retryPolicy the retry policy to use
     */
    protected GenericDistributedAtomic(CuratorFramework client, String counterPath, RetryPolicy retryPolicy)
    {
        this(client, counterPath, retryPolicy, null);
    }

    /**
     * Creates the counter with in mutex promotion mode. The optimistic lock will be tried first using
     * the given retry policy. If the increment does not succeed, a {@link InterProcessMutex} will be tried
     * with its own retry policy
     *
     * @param client the client
     * @param counterPath path to hold the counter value
     * @param retryPolicy the retry policy to use
     * @param promotedToLock the arguments for the mutex promotion
     */
    protected GenericDistributedAtomic(CuratorFramework client, String counterPath, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        this.client = client;
        this.counterPath = counterPath;
        this.retryPolicy = retryPolicy;
        this.promotedToLock = promotedToLock;
        mutex = (promotedToLock != null) ? new InterProcessMutex(client, promotedToLock.getPath()) : null;
    }

    /**
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<T>     get() throws Exception
    {
        MutableAtomicValue<T>       result = numberHelper().newMutableAtomicValue();
        getCurrentValue(result, new Stat());
        result.postValue = result.preValue;
        result.succeeded = true;
        return result;
    }

    @Override
    public void forceSet(T newValue) throws Exception
    {
        byte[] bytes = numberHelper().valueToBytes(newValue);
        try
        {
            client.setData().forPath(counterPath, bytes);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            try
            {
                client.create().forPath(counterPath, bytes);
            }
            catch ( KeeperException.NodeExistsException dummy2 )
            {
                client.setData().forPath(counterPath, bytes);
            }
        }
    }

    @Override
    public AtomicValue<T> compareAndSet(T expectedValue, T newValue) throws Exception
    {
        Stat                    stat = new Stat();
        MutableAtomicValue<T>   result = numberHelper().newMutableAtomicValue();
        boolean                 createIt = getCurrentValue(result, stat);
        if ( !createIt && expectedValue.equals(result.preValue) )
        {
            byte[]      newData = numberHelper().valueToBytes(newValue);
            try
            {
                client.setData().withVersion(stat.getVersion()).forPath(counterPath, newData);
                result.succeeded = true;
                result.postValue = newValue;
            }
            catch ( KeeperException.BadVersionException dummy )
            {
                result.succeeded = false;
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                result.succeeded = false;
            }
        }
        else
        {
            result.succeeded = false;
        }
        return result;
    }

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<T>    increment() throws Exception
    {
        return worker(numberHelper().one());
    }

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<T>    decrement() throws Exception
    {
        return worker(numberHelper().negativeOne());
    }

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<T>    add(T delta) throws Exception
    {
        return worker(delta);
    }

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<T> subtract(T delta) throws Exception
    {
        return worker(numberHelper().negate(delta));
    }

    private AtomicValue<T>   worker(T addAmount) throws Exception
    {
        MutableAtomicValue<T> result = numberHelper().newMutableAtomicValue();

        tryOptimistic(result, addAmount);
        if ( !result.succeeded() && (mutex != null) )
        {
            tryWithMutex(result, addAmount);
        }

        return result;
    }

    private void tryWithMutex(MutableAtomicValue<T> result, T addAmount) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        int             retryCount = 0;

        if ( mutex.acquire(promotedToLock.getMaxLockTime(), promotedToLock.getMaxLockTimeUnit()) )
        {
            try
            {
                boolean         done = false;
                while ( !done )
                {
                    result.stats.incrementPromotedTries();
                    if ( tryOnce(result, addAmount) )
                    {
                        result.succeeded = true;
                        done = true;
                    }
                    else
                    {
                        if ( !promotedToLock.getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMs) )
                        {
                            done = true;
                        }
                    }
                }
            }
            finally
            {
                mutex.release();
            }
        }

        result.stats.setPromotedTimeMs(System.currentTimeMillis() - startMs);
    }

    private void tryOptimistic(MutableAtomicValue<T> result, T addAmount) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        int             retryCount = 0;

        boolean         done = false;
        while ( !done )
        {
            result.stats.incrementOptimisticTries();
            if ( tryOnce(result, addAmount) )
            {
                result.succeeded = true;
                done = true;
            }
            else
            {
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs) )
                {
                    done = true;
                }
            }
        }

        result.stats.setOptimisticTimeMs(System.currentTimeMillis() - startMs);
    }

    private boolean tryOnce(MutableAtomicValue<T> result, T addAmount) throws Exception
    {
        Stat        stat = new Stat();
        boolean     createIt = getCurrentValue(result, stat);

        result.postValue = numberHelper().add(result.preValue, addAmount);
        T           newValue = result.postValue();


        byte[]      newData = numberHelper().valueToBytes(newValue);
        boolean     success = false;
        try
        {
            if ( createIt )
            {
                client.create().forPath(counterPath, newData);
            }
            else
            {
                client.setData().withVersion(stat.getVersion()).forPath(counterPath, newData);
            }
            success = true;
        }
        catch ( KeeperException.NodeExistsException e )
        {
            // do Retry
        }
        catch ( KeeperException.BadVersionException e )
        {
            // do Retry
        }
        catch ( KeeperException.NoNodeException e )
        {
            // do Retry
        }

        return success;
    }

    protected abstract NumberHelper<T>  numberHelper();

    private boolean getCurrentValue(MutableAtomicValue<T> result, Stat stat) throws Exception
    {
        boolean             createIt = false;
        try
        {
            byte[]                      data = client.getData().storingStatIn(stat).forPath(counterPath);
            result.preValue = numberHelper().bytesToValue(data);
        }
        catch ( KeeperException.NoNodeException e )
        {
            result.preValue = numberHelper().newMutableAtomicValue().preValue;
            createIt = true;
        }
        return createIt;
    }
}
