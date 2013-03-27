/**
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
package org.apache.curator.framework.recipes.atomic;

import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import java.util.Arrays;

/**
 * <p>A distributed value that attempts atomic sets. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the methods (other than get()) are guaranteed to succeed.</p>
 */
public class DistributedAtomicValue
{
    private final CuratorFramework  client;
    private final String            path;
    private final RetryPolicy       retryPolicy;
    private final PromotedToLock    promotedToLock;
    private final InterProcessMutex mutex;
    private final EnsurePath        ensurePath;

    /**
     * Creates in optimistic mode only - i.e. the promotion to a mutex is not done
     *
     * @param client the client
     * @param path path to hold the value
     * @param retryPolicy the retry policy to use
     */
    public DistributedAtomicValue(CuratorFramework client, String path, RetryPolicy retryPolicy)
    {
        this(client, path, retryPolicy, null);
    }

    /**
     * Creates in mutex promotion mode. The optimistic lock will be tried first using
     * the given retry policy. If the increment does not succeed, a {@link InterProcessMutex} will be tried
     * with its own retry policy
     *
     * @param client the client
     * @param path path to hold the value
     * @param retryPolicy the retry policy to use
     * @param promotedToLock the arguments for the mutex promotion
     */
    public DistributedAtomicValue(CuratorFramework client, String path, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        this.client = client;
        this.path = path;
        this.retryPolicy = retryPolicy;
        this.promotedToLock = promotedToLock;
        mutex = (promotedToLock != null) ? new InterProcessMutex(client, promotedToLock.getPath()) : null;
        ensurePath = client.newNamespaceAwareEnsurePath(path).excludingLast();
    }

    /**
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]>     get() throws Exception
    {
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);
        getCurrentValue(result, new Stat());
        result.postValue = result.preValue;
        result.succeeded = true;
        return result;
    }

    /**
     * Forcibly sets the value any guarantees of atomicity.
     *
     * @param newValue the new value
     * @throws Exception ZooKeeper errors
     */
    public void forceSet(byte[] newValue) throws Exception
    {
        try
        {
            ensurePath.ensure(client.getZookeeperClient());
            client.setData().forPath(path, newValue);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            try
            {
                client.create().forPath(path, newValue);
            }
            catch ( KeeperException.NodeExistsException dummy2 )
            {
                client.setData().forPath(path, newValue);
            }
        }
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * Remember to always check {@link AtomicValue#succeeded()}.
     *
     *
     * @param expectedValue the expected value
     * @param newValue the new value
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]> compareAndSet(byte[] expectedValue, byte[] newValue) throws Exception
    {
        Stat                        stat = new Stat();
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);
        boolean                     createIt = getCurrentValue(result, stat);
        if ( !createIt && Arrays.equals(expectedValue, result.preValue) )
        {
            try
            {
                client.setData().withVersion(stat.getVersion()).forPath(path, newValue);
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
     * Attempt to atomically set the value to the given value. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param newValue the value to set
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]>   trySet(final byte[] newValue) throws Exception
    {
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);

        MakeValue                   makeValue = new MakeValue()
        {
            @Override
            public byte[] makeFrom(byte[] previous)
            {
                return newValue;
            }
        };
        tryOptimistic(result, makeValue);
        if ( !result.succeeded() && (mutex != null) )
        {
            tryWithMutex(result, makeValue);
        }

        return result;
    }

    AtomicValue<byte[]>   trySet(MakeValue makeValue) throws Exception
    {
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);

        tryOptimistic(result, makeValue);
        if ( !result.succeeded() && (mutex != null) )
        {
            tryWithMutex(result, makeValue);
        }

        return result;
    }

    RuntimeException createCorruptionException(byte[] bytes)
    {
        StringBuilder       str = new StringBuilder();
        str.append('[');
        boolean             first = true;
        for ( byte b : bytes )
        {
            if ( first )
            {
                first = false;
            }
            else
            {
                str.append(", ");
            }
            str.append("0x").append(Integer.toHexString((b & 0xff)));
        }
        str.append(']');
        return new RuntimeException(String.format("Corrupted data for node \"%s\": %s", path, str.toString()));
    }

    private boolean getCurrentValue(MutableAtomicValue<byte[]> result, Stat stat) throws Exception
    {
        boolean             createIt = false;
        try
        {
            ensurePath.ensure(client.getZookeeperClient());
            result.preValue = client.getData().storingStatIn(stat).forPath(path);
        }
        catch ( KeeperException.NoNodeException e )
        {
            result.preValue = null;
            createIt = true;
        }
        return createIt;
    }

    private void tryWithMutex(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
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
                     if ( tryOnce(result, makeValue) )
                     {
                         result.succeeded = true;
                         done = true;
                     }
                     else
                     {
                         if ( !promotedToLock.getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
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

    private void tryOptimistic(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        int             retryCount = 0;

        boolean         done = false;
        while ( !done )
        {
            result.stats.incrementOptimisticTries();
            if ( tryOnce(result, makeValue) )
            {
                result.succeeded = true;
                done = true;
            }
            else
            {
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    done = true;
                }
            }
        }

        result.stats.setOptimisticTimeMs(System.currentTimeMillis() - startMs);
    }

    private boolean tryOnce(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
    {
        Stat        stat = new Stat();
        boolean     createIt = getCurrentValue(result, stat);

        boolean     success = false;
        try
        {
            byte[]  newValue = makeValue.makeFrom(result.preValue);
            if ( createIt )
            {
                client.create().forPath(path, newValue);
            }
            else
            {
                client.setData().withVersion(stat.getVersion()).forPath(path, newValue);
            }
            result.postValue = Arrays.copyOf(newValue, newValue.length);
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
}
