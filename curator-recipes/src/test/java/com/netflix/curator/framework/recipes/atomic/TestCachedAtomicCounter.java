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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
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
            AtomicNumber<Long> dal = new AtomicNumber<Long>()
            {
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
                Assert.assertTrue(value.succeeded());
                Assert.assertEquals(value.preValue().longValue(), i);
                Assert.assertEquals(value.postValue().longValue(), i + 1);

                if ( i == 0 )
                {
                    MutableAtomicValue<Long> badValue = new MutableAtomicValue<Long>(0L, 0L);
                    badValue.succeeded = false;
                    fakeValueRef.set(badValue);
                }
            }

            value = cachedLong.next();
            Assert.assertFalse(value.succeeded());
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
                Assert.assertTrue(value.succeeded());
                Assert.assertEquals(value.preValue().longValue(), i);
                Assert.assertEquals(value.postValue().longValue(), i + 1);
            }
        }
        finally
        {
            client.close();
        }
    }
}
