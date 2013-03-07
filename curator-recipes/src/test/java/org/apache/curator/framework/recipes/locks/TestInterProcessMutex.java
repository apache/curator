/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestInterProcessMutex extends TestInterProcessMutexBase
{
    private static final String LOCK_PATH = "/locks/our-lock";

    @Override
    protected InterProcessLock makeLock(CuratorFramework client)
    {
        return new InterProcessMutex(client, LOCK_PATH);
    }

    @Test
    public void     testRevoking() throws Exception
    {
        final CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            final InterProcessMutex       lock = new InterProcessMutex(client, LOCK_PATH);

            ExecutorService               executorService = Executors.newCachedThreadPool();

            final CountDownLatch          revokeLatch = new CountDownLatch(1);
            final CountDownLatch          lockLatch = new CountDownLatch(1);
            Future<Void>                  f1 = executorService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        RevocationListener<InterProcessMutex> listener = new RevocationListener<InterProcessMutex>()
                        {
                            @Override
                            public void revocationRequested(InterProcessMutex lock)
                            {
                                revokeLatch.countDown();
                            }
                        };
                        lock.makeRevocable(listener);
                        lock.acquire();
                        lockLatch.countDown();
                        revokeLatch.await();
                        lock.release();
                        return null;
                    }
                }
            );

            Future<Void>                  f2 = executorService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        Assert.assertTrue(lockLatch.await(10, TimeUnit.SECONDS));
                        Collection<String> nodes = lock.getParticipantNodes();
                        Assert.assertEquals(nodes.size(), 1);
                        Revoker.attemptRevoke(client, nodes.iterator().next());

                        InterProcessMutex       l2 = new InterProcessMutex(client, LOCK_PATH);
                        Assert.assertTrue(l2.acquire(5, TimeUnit.SECONDS));
                        l2.release();
                        return null;
                    }
                }
            );

            f2.get();
            f1.get();
        }
        finally
        {
            client.close();
        }
    }
}
