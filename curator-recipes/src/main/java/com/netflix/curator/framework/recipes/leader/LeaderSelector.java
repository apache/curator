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
package com.netflix.curator.framework.recipes.leader;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
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
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final LeaderSelectorListener    listener;
    private final ExecutorService           executorService;
    private final Executor                  executor;
    private final InterProcessMutex         mutex;

    private volatile boolean                hasLeadership;
    private volatile String                 id = "";

    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryBuilder().setNameFormat("LeaderSelector-%d").build();

    /**
     * @param client the client
     * @param mutexPath the path for this leadership group
     * @param listener listener
     */
    public LeaderSelector(CuratorFramework client, String mutexPath, LeaderSelectorListener listener)
    {
        this(client, mutexPath, defaultThreadFactory, MoreExecutors.sameThreadExecutor(), listener);
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
        this.executor = executor;
        hasLeadership = false;

        executorService = Executors.newFixedThreadPool(1, threadFactory);
        mutex = new InterProcessMutex(client, mutexPath)
        {
            @Override
            protected byte[] getLockNodeBytes()
            {
                try
                {
                    return id.getBytes("UTF-8");
                }
                catch ( UnsupportedEncodingException e )
                {
                    throw new Error(e); // this should never happen
                }
            }
        };
    }

    /**
     * Sets the ID to store for this leader. Will be the value returned
     * when {@link #getParticipants()} is called. IMPORTANT: must be called
     * prior to {@link #start()} to have effect.
     *
     * @param id ID
     */
    public void     setId(String id)
    {
        Preconditions.checkNotNull(id);
        
        this.id = id;
    }

    /**
     * Return the ID that was set via {@link #setId(String)}
     *
     * @return id
     */
    public String getId()
    {
        return id;
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

        client.getConnectionStateListenable().addListener(listener);
        executorService.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    doWork();
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

        client.getConnectionStateListenable().removeListener(listener);
        executorService.shutdownNow();
    }

    /**
     * <p>
     *     Returns the set of current participants in the leader selection
     * </p>
     *
     * <p>
     *     <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     *     return a value that does not match {@link #hasLeadership()} as hasLeadership
     *     uses a local field of the class.
     * </p>
     *
     * @return participants
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Participant>  getParticipants() throws Exception
    {
        ImmutableList.Builder<Participant> builder = ImmutableList.builder();

        boolean         isLeader = true;
        for ( String path : mutex.getParticipantNodes() )
        {
            try
            {
                Participant     participant = participantForPath(path, isLeader);
                builder.add(participant);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }

            isLeader = false;   // by definition the first node is the leader
        }

        return builder.build();
    }

    /**
     * <p>
     *     Return the id for the current leader. If for some reason there is no
     *     current leader, a dummy participant is returned.
     * </p>
     *
     * <p>
     *     <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     *     return a value that does not match {@link #hasLeadership()} as hasLeadership
     *     uses a local field of the class.
     * </p>
     *
     * @return leader
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Participant      getLeader() throws Exception
    {
        Collection<String>      participantNodes = mutex.getParticipantNodes();
        if ( participantNodes.size() > 0 )
        {
            return participantForPath(participantNodes.iterator().next(), true);
        }
        return new Participant();
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

    private Participant participantForPath(String path, boolean markAsLeader) throws Exception
    {
        byte[]      bytes = client.getData().forPath(path);
        String      thisId = new String(bytes, "UTF-8");
        return new Participant(thisId, markAsLeader);
    }

    private void doWork() throws Exception
    {
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
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        catch ( Exception e )
                        {
                            log.error("The leader threw an exception", e);
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
