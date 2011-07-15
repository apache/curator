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
package com.netflix.curator.framework.recipes.mutex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.PathAndBytesable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
 */
public class InterProcessMutex
{
    private final CuratorFramework      client;
    private final String                path;
    private final Watcher               watcher;
    private final String                basePath;
    private final CuratorListener       listener;
    private final ClientClosingListener clientClosingListener;
    private final AtomicBoolean         basePathEnsured = new AtomicBoolean(false);

    private volatile LockData           lockData;

    private static class LockData
    {
        volatile Thread     owningThread;
        volatile String     lockPath;
        volatile int        lockCount;
    }

    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path the path to lock
     */
    public InterProcessMutex(CuratorFramework client, String path)
    {
        this(client, path, null);
    }

    /**
     * @param client client
     * @param path the path to lock
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     */
    public InterProcessMutex(CuratorFramework client, String path, ClientClosingListener clientClosingListener)
    {
        this.clientClosingListener = clientClosingListener;
        Preconditions.checkArgument(!path.endsWith("/"));

        this.client = client;
        basePath = path;
        this.path = makePath(path);
        lockData = null;

        watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent watchedEvent)
            {
                notifyFromWatcher();
            }
        };

        listener = new CuratorListener()
        {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getType() == CuratorEventType.CLOSING )
                {
                    handleClosingEvent();
                }
            }

            @Override
            public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
            {
                // NOP - closing event will have been sent
            }
        };
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     * 
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public void acquire() throws Exception
    {
        internalLock(-1, null);
        if ( !isAcquiredInThisProcess() )
        {
            throw new IOException("Lost connection while trying to acquire lock");
        }
    }

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        internalLock(time, unit);
        if ( isAcquiredInThisProcess() )
        {
            if ( lockData.lockCount == 1 )
            {
                client.addListener(listener);
            }
            return true;
        }
        return false;
    }

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    public boolean isAcquiredInThisProcess()
    {
        return lockData != null;
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    public void release() throws Exception
    {
        if ( (lockData == null) || (lockData.owningThread != Thread.currentThread()) )
        {
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }

        if ( --lockData.lockCount > 0 )
        {
            return;
        }
        if ( lockData.lockCount < 0 )
        {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }

        client.removeListener(listener);

        client.delete().forPath(lockData.lockPath);
        lockData = null;

        // attempt to delete the parent node so that sequence numbers get reset
        try
        {
            Stat stat = client.checkExists().forPath(basePath);
            if ( (stat != null) && (stat.getNumChildren() == 0) )
            {
                client.delete().withVersion(stat.getVersion()).forPath(basePath);
            }
        }
        catch ( KeeperException.BadVersionException ignore )
        {
            // ignore - another thread/process got the lock
        }
        catch ( KeeperException.NotEmptyException ignore )
        {
            // ignore - other threads/processes are waiting
        }
    }

    private void internalLock(long time, TimeUnit unit) throws Exception
    {
        boolean     re_entering = (lockData != null) && (lockData.owningThread == Thread.currentThread());
        if ( !re_entering && (lockData != null) )
        {
            throw new IllegalMonitorStateException("This lock: " + basePath + " is already owned by " + lockData.owningThread);
        }

        if ( re_entering )
        {
            ++lockData.lockCount;
            return;
        }

        LockData        localLockData = new LockData();
        localLockData.lockCount = 1;
        localLockData.owningThread = Thread.currentThread();
        localLockData.lockPath = null;

        long startMillis = System.currentTimeMillis();
        Long millisToWait = (unit != null) ? unit.toMillis(time) : null;

        PathAndBytesable<String> createBuilder;
        if ( basePathEnsured.get() )    // optimization - avoids calling creatingParentsIfNeeded() unnecessarily - first concurrent threads will do it, but not others
        {
            createBuilder = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        else 
        {
            createBuilder = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        String          ourPath = createBuilder.forPath(path, new byte[0]);
        basePathEnsured.set(true);

        try
        {
            while ( client.isStarted() && (lockData == null) )
            {
                List<String> children = getSortedChildren(basePath);
                String sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                int ourIndex = children.indexOf(sequenceNodeName);
                if ( ourIndex < 0 )
                {
                    throw new Exception("Sequential path not found: " + ourPath);
                }

                if ( ourIndex == 0 )
                {
                    // we have the lock
                    localLockData.lockPath = ourPath;
                    lockData = localLockData;
                }
                else
                {
                    String previousSequenceNodeName = children.get(ourIndex - 1);
                    String previousSequencePath = basePath + "/" + previousSequenceNodeName;
                    synchronized(this)
                    {
                        Stat stat = client.checkExists().usingWatcher(watcher).forPath(previousSequencePath);
                        if ( stat != null )
                        {
                            if ( millisToWait != null )
                            {
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if ( millisToWait <= 0 )
                                {
                                    break;
                                }

                                wait(millisToWait);
                            }
                            else
                            {
                                wait();
                            }
                        }
                    }
                    // else it may have been deleted (i.e. lock released). Try to acquire again
                }
            }
        }
        catch ( Exception e )
        {
            client.delete().forPath(ourPath);
            throw e;
        }
    }

    private void handleClosingEvent()
    {
        if ( clientClosingListener != null )
        {
            clientClosingListener.notifyClientClosing(this, client);
        }
        notifyFromWatcher();
    }

    private synchronized void notifyFromWatcher()
    {
        notifyAll();
    }

    private static String makePath(String path)
    {
        if ( !path.endsWith("/") )
        {
            path += "/";
        }
        return path + LOCK_NAME;
    }

    private List<String> getSortedChildren(String path)
        throws Exception
    {
        List<String> children = client.getChildren().forPath(path);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort(sortedList);
        return sortedList;
    }
}
