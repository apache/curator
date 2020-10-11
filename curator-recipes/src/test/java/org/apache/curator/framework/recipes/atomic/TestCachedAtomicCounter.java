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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

public class TestCachedAtomicCounter extends BaseClassForTests
{
    @Test
    public void         testWithError() throws Exception
    {
        final int        FACTOR = 100;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            AtomicValue<Long>                           value = new MutableAtomicValue<Long>(0L, (long)FACTOR, true);
            final AtomicReference<AtomicValue<Long>>    fakeValueRef = new AtomicReference<AtomicValue<Long>>(value);
            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/", null, null)
            {
                @Override
                public AtomicValue<Long> trySet(Long newValue) throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public AtomicValue<Long> get() throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public AtomicValue<Long> increment() throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public AtomicValue<Long> decrement() throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public AtomicValue<Long> add(Long delta) throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public AtomicValue<Long> subtract(Long delta) throws Exception
                {
                    return fakeValueRef.get();
                }

                @Override
                public void forceSet(Long newValue) throws Exception
                {
                }

                @Override
                public AtomicValue<Long> compareAndSet(Long expectedValue, Long newValue) throws Exception
                {
                    return fakeValueRef.get();
                }
            };
            CachedAtomicLong cachedLong = new CachedAtomicLong(dal, FACTOR);
            for ( int i = 0; i < FACTOR; ++i )
            {
                value = cachedLong.next();
                assertTrue(value.succeeded());
                assertEquals(value.preValue().longValue(), i);
                assertEquals(value.postValue().longValue(), i + 1);

                if ( i == 0 )
                {
                    MutableAtomicValue<Long> badValue = new MutableAtomicValue<Long>(0L, 0L);
                    badValue.succeeded = false;
                    fakeValueRef.set(badValue);
                }
            }

            value = cachedLong.next();
            assertFalse(value.succeeded());
        }
        finally
        {
            client.close();
        }
        }

    @Test
    public void         testBasic() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            DistributedAtomicLong dal = new DistributedAtomicLong(client, "/counter", new RetryOneTime(1));
            CachedAtomicLong cachedLong = new CachedAtomicLong(dal, 100);
            for ( long i = 0; i < 200; ++i )
            {
                AtomicValue<Long>       value = cachedLong.next();
                assertTrue(value.succeeded());
                assertEquals(value.preValue().longValue(), i);
                assertEquals(value.postValue().longValue(), i + 1);
            }
        }
        finally
        {
            client.close();
        }
    }
}
