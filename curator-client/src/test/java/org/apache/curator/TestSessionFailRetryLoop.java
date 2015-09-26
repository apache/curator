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
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestSessionFailRetryLoop extends BaseClassForTests
{
    @Test
    public void     testRetry() throws Exception
    {
        Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new ExponentialBackoffRetry(100, 3));
        SessionFailRetryLoop            retryLoop = client.newSessionFailRetryLoop(SessionFailRetryLoop.Mode.RETRY);
        retryLoop.start();
        try
        {
            client.start();
            final AtomicBoolean     secondWasDone = new AtomicBoolean(false);
            final AtomicBoolean     firstTime = new AtomicBoolean(true);
            while ( retryLoop.shouldContinue() )
            {
                try
                {
                    RetryLoop.callWithRetry
                    (
                        client,
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                if ( firstTime.compareAndSet(true, false) )
                                {
                                    Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                    KillSession.kill(client.getZooKeeper(), server.getConnectString());
                                    client.getZooKeeper();
                                    client.blockUntilConnectedOrTimedOut();
                                }

                                Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                return null;
                            }
                        }
                    );

                    RetryLoop.callWithRetry
                    (
                        client,
                        new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                Assert.assertFalse(firstTime.get());
                                Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                secondWasDone.set(true);
                                return null;
                            }
                        }
                    );
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }

            Assert.assertTrue(secondWasDone.get());
        }
        finally
        {
            retryLoop.close();
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testRetryStatic() throws Exception
    {
        Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new ExponentialBackoffRetry(100, 3));
        SessionFailRetryLoop            retryLoop = client.newSessionFailRetryLoop(SessionFailRetryLoop.Mode.RETRY);
        retryLoop.start();
        try
        {
            client.start();
            final AtomicBoolean     secondWasDone = new AtomicBoolean(false);
            final AtomicBoolean     firstTime = new AtomicBoolean(true);
            SessionFailRetryLoop.callWithRetry
            (
                client,
                SessionFailRetryLoop.Mode.RETRY,
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        RetryLoop.callWithRetry
                        (
                            client,
                            new Callable<Void>()
                            {
                                @Override
                                public Void call() throws Exception
                                {
                                    if ( firstTime.compareAndSet(true, false) )
                                    {
                                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                        KillSession.kill(client.getZooKeeper(), server.getConnectString());
                                        client.getZooKeeper();
                                        client.blockUntilConnectedOrTimedOut();
                                    }

                                    Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                    return null;
                                }
                            }
                        );

                        RetryLoop.callWithRetry
                        (
                            client,
                            new Callable<Void>()
                            {
                                @Override
                                public Void call() throws Exception
                                {
                                    Assert.assertFalse(firstTime.get());
                                    Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                    secondWasDone.set(true);
                                    return null;
                                }
                            }
                        );
                        return null;
                    }
                }
            );

            Assert.assertTrue(secondWasDone.get());
        }
        finally
        {
            retryLoop.close();
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        final Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new ExponentialBackoffRetry(100, 3));
        SessionFailRetryLoop            retryLoop = client.newSessionFailRetryLoop(SessionFailRetryLoop.Mode.FAIL);
        retryLoop.start();
        try
        {
            client.start();
            try
            {
                while ( retryLoop.shouldContinue() )
                {
                    try
                    {
                        RetryLoop.callWithRetry
                        (
                            client,
                            new Callable<Void>()
                            {
                                @Override
                                public Void call() throws Exception
                                {
                                    Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                    KillSession.kill(client.getZooKeeper(), server.getConnectString());

                                    timing.sleepABit();

                                    client.getZooKeeper();
                                    client.blockUntilConnectedOrTimedOut();
                                    Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                    return null;
                                }
                            }
                        );
                    }
                    catch ( Exception e )
                    {
                        retryLoop.takeException(e);
                    }
                }

                Assert.fail();
            }
            catch ( SessionFailRetryLoop.SessionFailedException dummy )
            {
                // correct
            }
        }
        finally
        {
            retryLoop.close();
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testBasicStatic() throws Exception
    {
        Timing                          timing = new Timing();
        final CuratorZookeeperClient    client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), null, new ExponentialBackoffRetry(100, 3));
        SessionFailRetryLoop            retryLoop = client.newSessionFailRetryLoop(SessionFailRetryLoop.Mode.FAIL);
        retryLoop.start();
        try
        {
            client.start();
            try
            {
                SessionFailRetryLoop.callWithRetry
                (
                    client,
                    SessionFailRetryLoop.Mode.FAIL,
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            RetryLoop.callWithRetry
                            (
                                client,
                                new Callable<Void>()
                                {
                                    @Override
                                    public Void call() throws Exception
                                    {
                                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                        KillSession.kill(client.getZooKeeper(), server.getConnectString());

                                        client.getZooKeeper();
                                        client.blockUntilConnectedOrTimedOut();
                                        Assert.assertNull(client.getZooKeeper().exists("/foo/bar", false));
                                        return null;
                                    }
                                }
                            );
                            return null;
                        }
                    }
                );
            }
            catch ( SessionFailRetryLoop.SessionFailedException dummy )
            {
                // correct
            }
        }
        finally
        {
            retryLoop.close();
            CloseableUtils.closeQuietly(client);
        }
    }
}
