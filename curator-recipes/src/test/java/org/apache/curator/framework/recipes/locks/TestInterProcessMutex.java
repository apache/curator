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
package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    
    /**
     * See CURATOR-79. If the mutex is interrupted while attempting to acquire a lock it is
     * possible for the zNode to be created in ZooKeeper, but for Curator to think that it
     * hasn't been. This causes the next call to acquire() to fail because the an orphaned
     * zNode has been left behind from the previous call.
     */
    @Test
    public void testInterruptedDuringAcquire() throws Exception
    {
        Timing timing = new Timing();
        final CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        final InterProcessMutex       lock = new InterProcessMutex(client, LOCK_PATH);
        
        final AtomicBoolean interruptOnError = new AtomicBoolean(true);
        
        ((CuratorFrameworkImpl)client).debugUnhandledErrorListener = new UnhandledErrorListener()
        {
            
            @Override
            public void unhandledError(String message, Throwable e)
            {
                if(interruptOnError.compareAndSet(true, false))
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        
        //The lock path needs to exist for the deadlock to occur.
        try {
            client.create().creatingParentsIfNeeded().forPath(LOCK_PATH);
        } catch(NodeExistsException e) {            
        }
        
        try
        {
            //Interrupt the current thread. This will cause ensurePath() to fail.
            //We need to reinterrupt in the debugUnhandledErrorListener above.
            Thread.currentThread().interrupt();
            lock.acquire();
            Assert.fail();
        }
        catch(InterruptedException e)
        {
            //Expected lock to have failed.
            Assert.assertTrue(!lock.isOwnedByCurrentThread());
        }
        
        try
        {
            Assert.assertTrue(lock.acquire(timing.seconds(), TimeUnit.SECONDS));
        }
        catch(Exception e)
        {
            Assert.fail();
        }
        finally
        {
            if(lock.isOwnedByCurrentThread())
            {
                lock.release();
            }
        }
    }
}
