/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.recipes.leader;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.ClientClosingListener;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected
 * to a Zookeeper cluster. If a group of N thread/processes contend for leadership one will randomly
 * be assigned leader until it releases leadership at which time another one from the group will randomly
 * be chosen
 */
public class LeaderSelector implements Closeable
{
    private final CuratorFramework          client;
    private final LeaderSelectorListener    listener;
    private final String                    mutexPath;
    private final ExecutorService           executorService;
    private final Executor                  executor;

    private volatile boolean                hasLeadership;

    /**
     * @param client the client
     * @param mutexPath the path for this leadership group
     * @param listener listener
     */
    public LeaderSelector(CuratorFramework client, String mutexPath, LeaderSelectorListener listener)
    {
        this(client, mutexPath, Executors.defaultThreadFactory(), MoreExecutors.sameThreadExecutor(), listener);
    }

    /**
     * @param client the client
     * @param mutexPath the path for this leadership group
     * @param threadFactory factory to use for making internal threads
     * @param executor the executor to run in
     * @param listener listener
     */
    public LeaderSelector(CuratorFramework client, String mutexPath, ThreadFactory threadFactory, Executor executor, LeaderSelectorListener listener)
    {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(mutexPath);
        Preconditions.checkNotNull(listener);

        this.client = client;
        this.listener = listener;
        this.mutexPath = mutexPath;
        this.executor = executor;
        hasLeadership = false;

        executorService = Executors.newFixedThreadPool(1, threadFactory);
    }

    /**
     * Attempt leadership. This attempt is done in the background - i.e. this method returns
     * immediately. Once you've been assigned leadership you can release it and call this method
     * again to re-obtain leadership
     */
    public void     start()
    {
        Preconditions.checkArgument(!executorService.isShutdown());
        Preconditions.checkArgument(!hasLeadership);

        executorService.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    Thread.currentThread().setName("LeaderSelector " + Thread.currentThread().getName());
                    runLoop();
                    return null;
                }
            }
        );
    }

    /**
     * Shutdown this selector and remove yourself from the leadership group
     */
    public void     close()
    {
        Preconditions.checkArgument(!executorService.isShutdown());

        executorService.shutdownNow();
    }

    /**
     * Return true if leadership is currently held by this instance
     *
     * @return true/false
     */
    public boolean hasLeadership()
    {
        return hasLeadership;
    }

    private void runLoop() throws Exception
    {
        ClientClosingListener<InterProcessMutex>   clientClosingListener = new ClientClosingListener<InterProcessMutex>()
        {
            @Override
            public void notifyClientClosing(InterProcessMutex lock, final CuratorFramework client)
            {
                executor.execute
                (
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            listener.notifyClientClosing(client);
                        }
                    }
                );
            }
        };
        InterProcessMutex       mutex = new InterProcessMutex(client, mutexPath, clientClosingListener);
        hasLeadership = false;
        try
        {
            mutex.acquire();

            hasLeadership = true;
            executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            listener.takeLeadership(client);
                        }
                        catch ( Exception e )
                        {
                            listener.handleException(client, e);
                        }
                    }
                }
            );
        }
        finally
        {
            hasLeadership = false;
            try
            {
                mutex.release();
            }
            catch ( Exception ignore )
            {
                // ignore errors - this is just a safety
            }
        }
    }
}
