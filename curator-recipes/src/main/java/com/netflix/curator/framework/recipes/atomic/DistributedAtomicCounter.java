package com.netflix.curator.framework.recipes.atomic;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import java.nio.ByteBuffer;

/**
 * <p>A counter that attempts atomic increments. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various increment methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the increment methods are guaranteed to succeed.</p>
 */
public class DistributedAtomicCounter
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
    public DistributedAtomicCounter(CuratorFramework client, String counterPath, RetryPolicy retryPolicy)
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
    public DistributedAtomicCounter(CuratorFramework client, String counterPath, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
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
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<Long>     get() throws Exception
    {
        MutableAtomicValue<Long>    result = new MutableAtomicValue<Long>(0L, 0L);
        boolean                     createIt = getCurrentValue(result, new Stat());
        if ( createIt )
        {
            result.preValue = 0L;
        }
        result.postValue = result.preValue;
        result.succeeded = true;
        return result;
    }

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<Long>    increment() throws Exception
    {
        return worker(1);
    }

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<Long>    decrement() throws Exception
    {
        return worker(-1);
    }

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<Long>    add(long delta) throws Exception
    {
        return worker(delta);
    }

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<Long>    subtract(long delta) throws Exception
    {
        return worker(-delta);
    }

    private AtomicValue<Long>   worker(long addAmount) throws Exception
    {
        MutableAtomicValue<Long> result = new MutableAtomicValue<Long>(0L, 0L);

        tryOptimistic(result, addAmount);
        if ( !result.succeeded() && (mutex != null) )
        {
            tryWithMutex(result, addAmount);
        }

        return result;
    }

    private void tryWithMutex(MutableAtomicValue<Long> result, long addAmount) throws Exception
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

    private void tryOptimistic(MutableAtomicValue<Long> result, long addAmount) throws Exception
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

    private boolean tryOnce(MutableAtomicValue<Long> result, long addAmount) throws Exception
    {
        Stat        stat = new Stat();
        boolean     createIt = getCurrentValue(result, stat);

        result.postValue = result.preValue + addAmount;

        byte[]                      newData = new byte[8];
        ByteBuffer                  wrapper = ByteBuffer.wrap(newData);
        wrapper.putLong(result.postValue());

        boolean                     success = false;
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

    private boolean getCurrentValue(MutableAtomicValue<Long> result, Stat stat) throws Exception
    {
        boolean             createIt = false;
        try
        {
            byte[]                      data = client.getData().storingStatIn(stat).forPath(counterPath);
            ByteBuffer wrapper = ByteBuffer.wrap(data);
            result.preValue = wrapper.getLong();
        }
        catch ( KeeperException.NoNodeException e )
        {
            result.preValue = 0L;
            createIt = true;
        }
        return createIt;
    }

}
