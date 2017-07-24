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

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.curator.connection.StandardConnectionHandlingPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEnsurePath
{
    @Test
    public void    testBasic() throws Exception
    {
        ZooKeeper               client = mock(ZooKeeper.class, Mockito.RETURNS_MOCKS);
        CuratorZookeeperClient  curator = mock(CuratorZookeeperClient.class);
        RetryPolicy             retryPolicy = new RetryOneTime(1);
        RetryLoop               retryLoop = new RetryLoop(retryPolicy, null);
        when(curator.getConnectionHandlingPolicy()).thenReturn(new StandardConnectionHandlingPolicy());
        when(curator.getZooKeeper()).thenReturn(client);
        when(curator.getRetryPolicy()).thenReturn(retryPolicy);
        when(curator.newRetryLoop()).thenReturn(retryLoop);

        Stat                    fakeStat = mock(Stat.class);
        when(client.exists(Mockito.<String>any(), anyBoolean())).thenReturn(fakeStat);
        
        EnsurePath      ensurePath = new EnsurePath("/one/two/three");
        ensurePath.ensure(curator);

        verify(client, times(3)).exists(Mockito.<String>any(), anyBoolean());

        ensurePath.ensure(curator);
        verifyNoMoreInteractions(client);
        ensurePath.ensure(curator);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void    testSimultaneous() throws Exception
    {
        ZooKeeper               client = mock(ZooKeeper.class, Mockito.RETURNS_MOCKS);
        RetryPolicy             retryPolicy = new RetryOneTime(1);
        RetryLoop               retryLoop = new RetryLoop(retryPolicy, null);
        final CuratorZookeeperClient  curator = mock(CuratorZookeeperClient.class);
        when(curator.getConnectionHandlingPolicy()).thenReturn(new StandardConnectionHandlingPolicy());
        when(curator.getZooKeeper()).thenReturn(client);
        when(curator.getRetryPolicy()).thenReturn(retryPolicy);
        when(curator.newRetryLoop()).thenReturn(retryLoop);

        final Stat              fakeStat = mock(Stat.class);
        final CountDownLatch    startedLatch = new CountDownLatch(2);
        final CountDownLatch    finishedLatch = new CountDownLatch(2);
        final Semaphore         semaphore = new Semaphore(0);
        when(client.exists(Mockito.<String>any(), anyBoolean())).thenAnswer
        (
            new Answer<Stat>()
            {
                @Override
                public Stat answer(InvocationOnMock invocation) throws Throwable
                {
                    semaphore.acquire();
                    return fakeStat;
                }
            }
        );

        final EnsurePath    ensurePath = new EnsurePath("/one/two/three");
        ExecutorService     service = Executors.newCachedThreadPool();
        for ( int i = 0; i < 2; ++i )
        {
            service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        startedLatch.countDown();
                        ensurePath.ensure(curator);
                        finishedLatch.countDown();
                        return null;
                    }
                }
            );
        }

        Assert.assertTrue(startedLatch.await(10, TimeUnit.SECONDS));
        semaphore.release(3);
        Assert.assertTrue(finishedLatch.await(10, TimeUnit.SECONDS));
        verify(client, times(3)).exists(Mockito.<String>any(), anyBoolean());

        ensurePath.ensure(curator);
        verifyNoMoreInteractions(client);
        ensurePath.ensure(curator);
        verifyNoMoreInteractions(client);
    }
}
