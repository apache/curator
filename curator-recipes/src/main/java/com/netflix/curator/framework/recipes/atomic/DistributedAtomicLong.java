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
import java.nio.ByteBuffer;

/**
 * <p>A counter that attempts atomic increments. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various increment methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the methods (other than get()) are guaranteed to succeed.</p>
 */
public class DistributedAtomicLong extends GenericDistributedAtomic<Long> implements NumberHelper<Long>
{
    public DistributedAtomicLong(CuratorFramework client, String counterPath, RetryPolicy retryPolicy)
    {
        super(client, counterPath, retryPolicy);
    }

    public DistributedAtomicLong(CuratorFramework client, String counterPath, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        super(client, counterPath, retryPolicy, promotedToLock);
    }

    @Override
    protected NumberHelper<Long> numberHelper()
    {
        return this;
    }

    @Override
    public byte[] valueToBytes(Long newValue)
    {
        byte[]                      newData = new byte[8];
        ByteBuffer wrapper = ByteBuffer.wrap(newData);
        wrapper.putLong(newValue);
        return newData;
    }

    @Override
    public Long bytesToValue(byte[] data)
    {
        ByteBuffer wrapper = ByteBuffer.wrap(data);
        return wrapper.getLong();
    }

    @Override
    public MutableAtomicValue<Long> newMutableAtomicValue()
    {
        return new MutableAtomicValue<Long>(0L, 0L);
    }

    @Override
    public Long one()
    {
        return 1L;
    }

    @Override
    public Long negativeOne()
    {
        return -1L;
    }

    @Override
    public Long negate(Long value)
    {
        return -1L * value;
    }

    @Override
    public Long add(Long a, Long b)
    {
        return a + b;
    }
}
