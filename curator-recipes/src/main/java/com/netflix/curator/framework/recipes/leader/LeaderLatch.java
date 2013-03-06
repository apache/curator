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

package com.netflix.curator.framework.recipes.leader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.recipes.locks.LockInternals;
import com.netflix.curator.framework.recipes.locks.LockInternalsSorter;
import com.netflix.curator.framework.recipes.locks.StandardLockInternalsDriver;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected to
 *     a Zookeeper cluster. If a group of N thread/processes contend for leadership one will
 *     randomly be assigned leader until it releases leadership at which time another one from the
 *     group will randomly be chosen
 * </p>
 */
public class LeaderLatch implements Closeable
{
    private final Logger                                log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework                      client;
    private final String                                latchPath;
    private final String                                id;
    private final AtomicReference<State>                state = new AtomicReference<State>(State.LATENT);
    private final AtomicBoolean                         hasLeadership = new AtomicBoolean(false);
    private final AtomicReference<String>               ourPath = new AtomicReference<String>();

    private final ConnectionStateListener               listener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    private static final String LOCK_NAME = "latch-";

    private static final LockInternalsSorter        sorter = new LockInternalsSorter()
    {
        @Override
        public String fixForSorting(String str, String lockName)
        {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client the client
     * @param latchPath the path for this leadership group
     */
    public LeaderLatch(CuratorFramework client, String latchPath)
    {
        this(client, latchPath, "");
    }

    /**
     * @param client the client
     * @param latchPath the path for this leadership group
     * @param id participant ID
     */
    public LeaderLatch(CuratorFramework client, String latchPath, String id)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.latchPath = Preconditions.checkNotNull(latchPath, "mutexPath cannot be null");
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
    }

    /**
     * Add this instance to the leadership election and attempt to acquire leadership.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        client.getConnectionStateListenable().addListener(listener);
        reset();
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership
     * is released. IMPORTANT: the only way to release leadership is by calling close(). All LeaderLatch
     * instances must eventually be closed.
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");

        try
        {
            setNode(null);
        }
        catch ( Exception e )
        {
            throw new IOException(e);
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(listener);
            setLeadership(false);
        }
    }

    /**
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted} or {@linkplain #close() closed}.</p>
     *
     * <p>If this instance already is the leader then this method returns immediately.</p>
     *
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul></p>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     * @throws EOFException if the instance is {@linkplain #close() closed}
     *         while waiting
     */
    public void await() throws InterruptedException, EOFException
    {
        synchronized(this)
        {
            while ( (state.get() == State.STARTED) && !hasLeadership.get() )
            {
                wait();
            }
        }
        if ( state.get() != State.STARTED )
        {
            throw new EOFException();
        }
    }

    /**
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted},
     * the specified waiting time elapses or the instance is {@linkplain #close() closed}.</p>
     *
     * <p>If this instance already is the leader then this method returns immediately
     * with the value {@code true}.</p>
     *
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of four things happen:
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The specified waiting time elapses.</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     *
     * <p>If the specified waiting time elapses or the instance is {@linkplain #close() closed}
     * then the value {@code false} is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.</p>
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false}
     *         if the waiting time elapsed before the count reached zero or the instances was closed
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        long        waitNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

        synchronized(this)
        {
            while ( (waitNanos > 0) && (state.get() == State.STARTED) && !hasLeadership.get() )
            {
                long        startNanos = System.nanoTime();
                TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
                long        elapsed = System.nanoTime() - startNanos;
                waitNanos -= elapsed;
            }
        }
        return hasLeadership();
    }

    /**
     * Return this instance's participant Id
     *
     * @return participant Id
     */
    public String getId()
    {
        return id;
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
    public Collection<Participant> getParticipants() throws Exception
    {
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getParticipants(client, participantNodes);
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
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getLeader(client, participantNodes);
    }

    /**
     * Return true if leadership is currently held by this instance
     *
     * @return true/false
     */
    public boolean hasLeadership()
    {
        return (state.get() == State.STARTED) && hasLeadership.get();
    }

    @VisibleForTesting
    volatile CountDownLatch debugResetWaitLatch = null;

    @VisibleForTesting
    void reset() throws Exception
    {
        setLeadership(false);
        setNode(null);

        BackgroundCallback          callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( debugResetWaitLatch != null )
                {
                    debugResetWaitLatch.await();
                    debugResetWaitLatch = null;
                }

                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    setNode(event.getName());
                    getChildren();
                }
                else
                {
                    log.error("getChildren() failed. rc = " + event.getResultCode());
                }
            }
        };
        client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground(callback).forPath(ZKPaths.makePath(latchPath, LOCK_NAME), LeaderSelector.getIdBytes(id));
    }

    private void checkLeadership(List<String> children) throws Exception
    {
        final String    localOurPath = ourPath.get();
        List<String>    sortedChildren = LockInternals.getSortedChildren(LOCK_NAME, sorter, children);
        int             ourIndex = (localOurPath != null) ? sortedChildren.indexOf(ZKPaths.getNodeFromPath(localOurPath)) : -1;
        if ( ourIndex < 0 )
        {
            log.error("Can't find our node. Resetting. Index: " + ourIndex);
            reset();
        }
        else if ( ourIndex == 0 )
        {
            setLeadership(true);
        }
        else
        {
            String          watchPath = sortedChildren.get(ourIndex - 1);
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if ( (state.get() == State.STARTED) && (event.getType() == Event.EventType.NodeDeleted) && (localOurPath != null) )
                    {
                        try
                        {
                            getChildren();
                        }
                        catch(Exception ex)
                        {
                            log.error("An error occurred checking the leadership.", ex);
                        }
                    }
                }
            };

            BackgroundCallback          callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                    {
                        // previous node is gone - reset
                        reset();
                    }
                }
            };
            client.checkExists().usingWatcher(watcher).inBackground(callback).forPath(ZKPaths.makePath(latchPath, watchPath));
        }
    }

    private void getChildren() throws Exception
    {
        BackgroundCallback          callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    checkLeadership(event.getChildren());
                }
            }
        };
        client.getChildren().inBackground(callback).forPath(latchPath);
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
            default:
            {
                // NOP
                break;
            }

            case RECONNECTED:
            {
                try
                {
                    reset();
                }
                catch ( Exception e )
                {
                    log.error("Could not reset leader latch", e);
                    setLeadership(false);
                }
                break;
            }

            case SUSPENDED:
            case LOST:
            {
                setLeadership(false);
                break;
            }
        }
    }

    private synchronized void setLeadership(boolean newValue)
    {
        hasLeadership.set(newValue);
        notifyAll();
    }

    private void setNode(String newValue) throws Exception
    {
        String      oldPath = ourPath.getAndSet(newValue);
        if ( oldPath != null )
        {
            client.delete().guaranteed().inBackground().forPath(oldPath);
        }
    }
}
