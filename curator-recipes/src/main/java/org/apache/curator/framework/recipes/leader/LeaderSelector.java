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
package org.apache.curator.framework.recipes.leader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ThreadUtils;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     Abstraction select a "leader" amongst multiple contenders in a group of JMVs connected
 *     to a Zookeeper cluster. If a group of N thread/processes contend for leadership, one will
 *     be assigned leader until it releases leadership at which time another one from the group will
 *     be chosen.
 * </p>
 *
 * <p>
 *     Note that this class uses an underlying {@link InterProcessMutex} and as a result leader
 *     election is "fair" - each user will become leader in the order originally requested
 *     (from ZK's point of view).
 * </p>
 *
 */
public class LeaderSelector implements Closeable
{
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final LeaderSelectorListener    listener;
    private final ExecutorService           executorService;
    private final Executor                  executor;
    private final InterProcessMutex         mutex;
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private final AtomicBoolean             autoRequeue = new AtomicBoolean(false);

    private volatile boolean                hasLeadership;
    private volatile String                 id = "";

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    // guarded by synchronization
    private boolean                isQueued = false;

    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("LeaderSelector");

    /**
     * @param client the client
     * @param leaderPath the path for this leadership group
     * @param listener listener
     */
    public LeaderSelector(CuratorFramework client, String leaderPath, LeaderSelectorListener listener)
    {
        this(client, leaderPath, defaultThreadFactory, MoreExecutors.sameThreadExecutor(), listener);
    }

    /**
     * @param client the client
     * @param leaderPath the path for this leadership group
     * @param threadFactory factory to use for making internal threads
     * @param executor the executor to run in
     * @param listener listener
     */
    public LeaderSelector(CuratorFramework client, String leaderPath, ThreadFactory threadFactory, Executor executor, LeaderSelectorListener listener)
    {
        Preconditions.checkNotNull(client, "client cannot be null");
        Preconditions.checkNotNull(leaderPath, "leaderPath cannot be null");
        Preconditions.checkNotNull(listener, "listener cannot be null");

        this.client = client;
        this.listener = listener;
        this.executor = executor;
        hasLeadership = false;

        executorService = Executors.newFixedThreadPool(1, threadFactory);
        mutex = new InterProcessMutex(client, leaderPath)
        {
            @Override
            protected byte[] getLockNodeBytes()
            {
                return (id.length() > 0) ? getIdBytes(id) : null;
            }
        };
    }

    static byte[] getIdBytes(String id)
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

    /**
     * By default, when {@link LeaderSelectorListener#takeLeadership(CuratorFramework)} returns, this
     * instance is not requeued. Calling this method puts the leader selector into a mode where it
     * will always requeue itself.
     */
    public void autoRequeue()
    {
        autoRequeue.set(true);
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
        Preconditions.checkNotNull(id, "id cannot be null");
        
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
     * immediately.<br/><br/>
     * <b>IMPORTANT: </b> previous versions allowed this method to be called multiple times. This
     * is no longer supported. Use {@link #requeue()} for this purpose.
     */
    public void     start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        Preconditions.checkState(!executorService.isShutdown(), "Already started");
        Preconditions.checkState(!hasLeadership, "Already has leadership");

        client.getConnectionStateListenable().addListener(listener);
        requeue();
    }

    /**
     * Re-queue an attempt for leadership. If this instance is already queued, nothing
     * happens and false is returned. If the instance was not queued, it is re-qeued and true
     * is returned
     *
     * @return true if re-queue is successful
     */
    public synchronized boolean     requeue()
    {
        Preconditions.checkState(state.get() == State.STARTED, "close() has already been called");

        if ( !isQueued )
        {
            isQueued = true;
            executorService.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        doWorkLoop();
                        return null;
                    }
                }
            );

            return true;
        }
        return false;
    }

    /**
     * Shutdown this selector and remove yourself from the leadership group
     */
    public void     close()
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");

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
        Collection<String> participantNodes = mutex.getParticipantNodes();

        return getParticipants(client, participantNodes);
    }

    static Collection<Participant> getParticipants(CuratorFramework client, Collection<String> participantNodes) throws Exception
    {
        ImmutableList.Builder<Participant> builder = ImmutableList.builder();

        boolean         isLeader = true;
        for ( String path : participantNodes )
        {
            try
            {
                Participant     participant = participantForPath(client, path, isLeader);
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
        return getLeader(client, participantNodes);
    }

    static Participant getLeader(CuratorFramework client, Collection<String> participantNodes) throws Exception
    {
        if ( participantNodes.size() > 0 )
        {
            return participantForPath(client, participantNodes.iterator().next(), true);
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

    private static Participant participantForPath(CuratorFramework client, String path, boolean markAsLeader) throws Exception
    {
        byte[]      bytes = client.getData().forPath(path);
        String      thisId = new String(bytes, "UTF-8");
        return new Participant(thisId, markAsLeader);
    }

    @VisibleForTesting
    void doWork() throws Exception
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
                        catch ( Throwable e )
                        {
                            log.error("The leader threw an exception", e);
                        }
                        finally
                        {
                            clearIsQueued();
                        }
                    }
                }
            );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Exception e )
        {
            log.error("mutex.acquire() threw an exception", e);
            throw e;
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

    private void doWorkLoop() throws Exception
    {
        do
        {
            KeeperException exception = null;
            try
            {
                doWork();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                exception = e;
            }
            catch ( KeeperException.SessionExpiredException e )
            {
                exception = e;
            }
            catch ( InterruptedException ignore )
            {
                Thread.currentThread().interrupt();
                break;
            }
            if ( (exception != null) && !autoRequeue.get() )   // autoRequeue should ignore connection loss or session expired and just keep trying
            {
                throw exception;
            }
        } while ( autoRequeue.get() && (state.get() == State.STARTED) && !Thread.currentThread().isInterrupted() );
    }

    private synchronized void clearIsQueued()
    {
        isQueued = false;
    }
}
