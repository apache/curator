/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * <p>A counter that attempts atomic increments. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various increment methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the methods (other than get()) are guaranteed to succeed.</p>
 */
public class DistributedAtomicInteger implements DistributedAtomicNumber<Integer>
{
    private final DistributedAtomicValue        value;

    /**
     * Creates in optimistic mode only - i.e. the promotion to a mutex is not done
     *
     * @param client the client
     * @param counterPath path to hold the value
     * @param retryPolicy the retry policy to use
     */
    public DistributedAtomicInteger(CuratorFramework client, String counterPath, RetryPolicy retryPolicy)
    {
        this(client, counterPath, retryPolicy, null);
    }

    /**
     * Creates in mutex promotion mode. The optimistic lock will be tried first using
     * the given retry policy. If the increment does not succeed, a {@link InterProcessMutex} will be tried
     * with its own retry policy
     *
     * @param client the client
     * @param counterPath path to hold the value
     * @param retryPolicy the retry policy to use
     * @param promotedToLock the arguments for the mutex promotion
     */
    public DistributedAtomicInteger(CuratorFramework client, String counterPath, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        value = new DistributedAtomicValue(client, counterPath, retryPolicy, promotedToLock);
    }

    @Override
    public AtomicValue<Integer>     get() throws Exception
    {
        return new AtomicInteger(value.get());
    }

    @Override
    public void forceSet(Integer newValue) throws Exception
    {
        value.forceSet(valueToBytes(newValue));
    }

    @Override
    public AtomicValue<Integer> compareAndSet(Integer expectedValue, Integer newValue) throws Exception
    {
        return new AtomicInteger(value.compareAndSet(valueToBytes(expectedValue), valueToBytes(newValue)));
    }

    @Override
    public AtomicValue<Integer>   trySet(Integer newValue) throws Exception
    {
        return new AtomicInteger(value.trySet(valueToBytes(newValue)));
    }

    @Override
    public boolean initialize(Integer initialize) throws Exception
    {
        return value.initialize(valueToBytes(initialize));
    }

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer>    increment() throws Exception
    {
        return worker(1);
    }

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer>    decrement() throws Exception
    {
        return worker(-1);
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
    public AtomicValue<Integer>    add(Integer delta) throws Exception
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
    public AtomicValue<Integer> subtract(Integer delta) throws Exception
    {
        return worker(-1 * delta);
    }

    @VisibleForTesting
    byte[] valueToBytes(Integer newValue)
    {
        Preconditions.checkNotNull(newValue, "newValue cannot be null");

        byte[]                      newData = new byte[4];
        ByteBuffer wrapper = ByteBuffer.wrap(newData);
        wrapper.putInt(newValue);
        return newData;
    }

    @VisibleForTesting
    int bytesToValue(byte[] data)
    {
        if ( (data == null) || (data.length == 0) )
        {
            return 0;
        }
        ByteBuffer wrapper = ByteBuffer.wrap(data);
        try
        {
            return wrapper.getInt();
        }
        catch ( BufferUnderflowException e )
        {
            throw value.createCorruptionException(data);
        }
        catch ( BufferOverflowException e )
        {
            throw value.createCorruptionException(data);
        }
    }

    private AtomicValue<Integer>   worker(final Integer addAmount) throws Exception
    {
        Preconditions.checkNotNull(addAmount, "addAmount cannot be null");

        MakeValue               makeValue = new MakeValue()
        {
            @Override
            public byte[] makeFrom(byte[] previous)
            {
                int        previousValue = (previous != null) ? bytesToValue(previous) : 0;
                int        newValue = previousValue + addAmount;
                return valueToBytes(newValue);
            }
        };

        AtomicValue<byte[]>     result = value.trySet(makeValue);
        return new AtomicInteger(result);
    }

    private class AtomicInteger implements AtomicValue<Integer>
    {
        private AtomicValue<byte[]> bytes;

        private AtomicInteger(AtomicValue<byte[]> bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public boolean succeeded()
        {
            return bytes.succeeded();
        }

        @Override
        public Integer preValue()
        {
            return bytesToValue(bytes.preValue());
        }

        @Override
        public Integer postValue()
        {
            return bytesToValue(bytes.postValue());
        }

        @Override
        public AtomicStats getStats()
        {
            return bytes.getStats();
        }
    }
}
