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
            AtomicCounter<Long>                         dal = new AtomicCounter<Long>()
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
            };
            CachedAtomicCounter         cachedCounter = new CachedAtomicCounter(dal, FACTOR);
            for ( int i = 0; i < FACTOR; ++i )
            {
                value = cachedCounter.next();
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

            value = cachedCounter.next();
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
            DistributedAtomicCounter    dal = new DistributedAtomicCounter(client, "/counter", new RetryOneTime(1));
            CachedAtomicCounter         cachedCounter = new CachedAtomicCounter(dal, 100);
            for ( long i = 0; i < 200; ++i )
            {
                AtomicValue<Long>       value = cachedCounter.next();
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
