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
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.utils.PathUtils;

/**
 * <p>
 * Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected
 * to a Zookeeper cluster. If a group of N thread/processes contends for leadership, one will
 * be assigned leader until it releases leadership at which time another one from the group will
 * be chosen.
 * </p>
 * <p>
 * Note that this class uses an underlying {@link InterProcessMutex} and as a result leader
 * election is "fair" - each user will become leader in the order originally requested
 * (from ZK's point of view).
 * </p>
 */
public class LeaderSelector implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final LeaderSelectorListener listener;
    private final CloseableExecutorService executorService;
    private final InterProcessMutex mutex;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final AtomicBoolean autoRequeue = new AtomicBoolean(false);
    private final AtomicReference<Future<?>> ourTask = new AtomicReference<Future<?>>(null);

    private volatile boolean hasLeadership;
    private volatile String id = "";

    @VisibleForTesting
    volatile CountDownLatch debugLeadershipLatch = null;
    volatile CountDownLatch debugLeadershipWaitLatch = null;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    // guarded by synchronization
    private boolean isQueued = false;

    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("LeaderSelector");

    /**
     * @param client     the client
     * @param leaderPath the path for this leadership group
     * @param listener   listener
     */
    public LeaderSelector(CuratorFramework client, String leaderPath, LeaderSelectorListener listener)
    {
        this(client, leaderPath, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true), listener);
    }

    /**
     * @param client        the client
     * @param leaderPath    the path for this leadership group
     * @param threadFactory factory to use for making internal threads
     * @param executor      the executor to run in
     * @param listener      listener
     * @deprecated This constructor was poorly thought out. Custom executor is useless. Use this version instead: {@link #LeaderSelector(CuratorFramework, String, ExecutorService, LeaderSelectorListener)}
     */
    @SuppressWarnings("UnusedParameters")
    @Deprecated
    public LeaderSelector(CuratorFramework client, String leaderPath, ThreadFactory threadFactory, Executor executor, LeaderSelectorListener listener)
    {
        this(client, leaderPath, new CloseableExecutorService(wrapExecutor(executor), true), listener);
    }

    /**
     * @param client          the client
     * @param leaderPath      the path for this leadership group
     * @param executorService thread pool to use
     * @param listener        listener
     */
    public LeaderSelector(CuratorFramework client, String leaderPath, ExecutorService executorService, LeaderSelectorListener listener)
    {
        this(client, leaderPath, new CloseableExecutorService(executorService), listener);
    }

    /**
     * @param client          the client
     * @param leaderPath      the path for this leadership group
     * @param executorService thread pool to use
     * @param listener        listener
     */
    public LeaderSelector(CuratorFramework client, String leaderPath, CloseableExecutorService executorService, LeaderSelectorListener listener)
    {
        Preconditions.checkNotNull(client, "client cannot be null");
        PathUtils.validatePath(leaderPath);
        Preconditions.checkNotNull(listener, "listener cannot be null");

        this.client = client;
        this.listener = new WrappedListener(this, listener);
        hasLeadership = false;

        this.executorService = executorService;
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
    public void setId(String id)
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
     * immediately.<br><br>
     * <b>IMPORTANT: </b> previous versions allowed this method to be called multiple times. This
     * is no longer supported. Use {@link #requeue()} for this purpose.
     */
    public void start()
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
    public boolean requeue()
    {
        Preconditions.checkState(state.get() == State.STARTED, "close() has already been called");
        return internalRequeue();
    }

    public synchronized boolean internalRequeue()
    {
        if ( !isQueued && (state.get() == State.STARTED) )
        {
            isQueued = true;
            Future<Void> task = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        doWorkLoop();
                    }
                    finally
                    {
                        clearIsQueued();
                        if ( autoRequeue.get() )
                        {
                            internalRequeue();
                        }
                    }
                    return null;
                }
            });
            ourTask.set(task);

            return true;
        }
        return false;
    }

    /**
     * Shutdown this selector and remove yourself from the leadership group
     */
    public synchronized void close()
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");

        client.getConnectionStateListenable().removeListener(listener);
        executorService.close();
        ourTask.set(null);
    }

    /**
     * <p>
     * Returns the set of current participants in the leader selection
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return participants
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Participant> getParticipants() throws Exception
    {
        Collection<String> participantNodes = mutex.getParticipantNodes();

        return getParticipants(client, participantNodes);
    }

    static Collection<Participant> getParticipants(CuratorFramework client, Collection<String> participantNodes) throws Exception
    {
        ImmutableList.Builder<Participant> builder = ImmutableList.builder();

        boolean isLeader = true;
        for ( String path : participantNodes )
        {
            try
            {
                Participant participant = participantForPath(client, path, isLeader);
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
     * Return the id for the current leader. If for some reason there is no
     * current leader, a dummy participant is returned.
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return leader
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Participant getLeader() throws Exception
    {
        Collection<String> participantNodes = mutex.getParticipantNodes();
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

    /**
     * Attempt to cancel and interrupt the current leadership if this instance has leadership
     */
    public synchronized void interruptLeadership()
    {
        Future<?> task = ourTask.get();
        if ( task != null )
        {
            task.cancel(true);
        }
    }

    private static Participant participantForPath(CuratorFramework client, String path, boolean markAsLeader) throws Exception
    {
        byte[] bytes = client.getData().forPath(path);
        String thisId = new String(bytes, "UTF-8");
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
            try
            {
                if ( debugLeadershipLatch != null )
                {
                    debugLeadershipLatch.countDown();
                }
                if ( debugLeadershipWaitLatch != null )
                {
                    debugLeadershipWaitLatch.await();
                }
                listener.takeLeadership(client);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw e;
            }
            catch ( Exception e )
            {
                log.error("The leader threw an exception", e);
            }
            finally
            {
                clearIsQueued();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw e;
        }
        catch ( Exception e )
        {
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
        }
        if ( (exception != null) && !autoRequeue.get() )   // autoRequeue should ignore connection loss or session expired and just keep trying
        {
            throw exception;
        }
    }

    private synchronized void clearIsQueued()
    {
        isQueued = false;
    }

    // temporary wrapper for deprecated constructor
    private static ExecutorService wrapExecutor(final Executor executor)
    {
        return new AbstractExecutorService()
        {
            private volatile boolean isShutdown = false;
            private volatile boolean isTerminated = false;

            @Override
            public void shutdown()
            {
                isShutdown = true;
            }

            @Override
            public List<Runnable> shutdownNow()
            {
                return Lists.newArrayList();
            }

            @Override
            public boolean isShutdown()
            {
                return isShutdown;
            }

            @Override
            public boolean isTerminated()
            {
                return isTerminated;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(Runnable command)
            {
                try
                {
                    executor.execute(command);
                }
                finally
                {
                    isShutdown = true;
                    isTerminated = true;
                }
            }
        };
    }

    private static class WrappedListener implements LeaderSelectorListener
    {
        private final LeaderSelector leaderSelector;
        private final LeaderSelectorListener listener;

        public WrappedListener(LeaderSelector leaderSelector, LeaderSelectorListener listener)
        {
            this.leaderSelector = leaderSelector;
            this.listener = listener;
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
            listener.takeLeadership(client);
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            try
            {
                listener.stateChanged(client, newState);
            }
            catch ( CancelLeadershipException dummy )
            {
                leaderSelector.interruptLeadership();
            }
        }
    }
}
