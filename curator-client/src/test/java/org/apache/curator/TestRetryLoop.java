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
package org.apache.curator;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;

public class TestRetryLoop extends BaseClassForTests
{
    @Test
    public void     testExponentialBackoffRetryLimit()
    {
        RetrySleeper                    sleeper = new RetrySleeper()
        {
            @Override
            public void sleepFor(long time, TimeUnit unit) throws InterruptedException
            {
                Assert.assertTrue(unit.toMillis(time) <= 100);
            }
        };
        ExponentialBackoffRetry         retry = new ExponentialBackoffRetry(1, Integer.MAX_VALUE, 100);
        for ( int i = 0; i >= 0; ++i )
        {
            retry.allowRetry(i, 0, sleeper);
        }
    }

    @Test
    public void     testRetryLoopWithFailure() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 5000, 5000, null, new RetryOneTime(1));
        client.start();
        try
        {
            int         loopCount = 0;
            RetryLoop   retryLoop = client.newRetryLoop();
            outer: while ( retryLoop.shouldContinue()  )
            {
                ++loopCount;
                switch ( loopCount )
                {
                    case 1:
                    {
                        server.stop();
                        break;
                    }

                    case 2:
                    {
                        server.restart();
                        break;
                    }

                    case 3:
                    case 4:
                    {
                        // ignore
                        break;
                    }

                    default:
                    {
                        Assert.fail();
                        break outer;
                    }
                }

                try
                {
                    client.blockUntilConnectedOrTimedOut();
                    client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.assertTrue(loopCount >= 2);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testRetryLoop() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            int         loopCount = 0;
            RetryLoop   retryLoop = client.newRetryLoop();
            while ( retryLoop.shouldContinue()  )
            {
                if ( ++loopCount > 2 )
                {
                    Assert.fail();
                    break;
                }

                try
                {
                    client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.assertTrue(loopCount > 0);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testRetryForever() throws Exception
    {
        int retryIntervalMs = 1;
        RetrySleeper sleeper = Mockito.mock(RetrySleeper.class);
        RetryForever retryForever = new RetryForever(retryIntervalMs);

        for (int i = 0; i < 10; i++)
        {
            boolean allowed = retryForever.allowRetry(i, 0, sleeper);
            Assert.assertTrue(allowed);
            Mockito.verify(sleeper, times(i + 1)).sleepFor(retryIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testRetryForeverWithSessionFailed() throws Exception
    {
        final Timing timing = new Timing();
        final RetryPolicy retryPolicy = new SessionFailedRetryPolicy(new RetryForever(1000));
        final CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, retryPolicy);
        client.start();

        try
        {
            int loopCount = 0;
            final RetryLoop retryLoop = client.newRetryLoop();
            while ( retryLoop.shouldContinue()  )
            {
                if ( ++loopCount > 1 )
                {
                    break;
                }

                try
                {
                    client.getZooKeeper().getTestable().injectSessionExpiration();
                    client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.fail("Should failed with SessionExpiredException.");
        }
        catch ( Exception e )
        {
            if ( e instanceof KeeperException )
            {
                int rc = ((KeeperException) e).code().intValue();
                Assert.assertEquals(rc, KeeperException.Code.SESSIONEXPIRED.intValue());
            }
            else
            {
                throw e;
            }
        }
        finally
        {
            client.close();
        }
    }
}
