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

package com.netflix.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.PathAndBytesable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class LockInternals<T>
{
    private final CuratorFramework          client;
    private final String                    path;
    private final String                    basePath;
    private final ClientClosingListener<T>  clientClosingListener;
    private final int                       numberOfLeases;
    private final CuratorListener           listener;
    private final AtomicBoolean             basePathEnsured = new AtomicBoolean(false);
    private final Watcher                   watcher;

    /**
     * Attempt to delete the lock node so that sequence numbers get reset
     *
     * @throws Exception errors
     */
    public void clean() throws Exception
    {
        try
        {
            client.delete().forPath(basePath);
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

    LockInternals(CuratorFramework client, String path, String lockName, ClientClosingListener<T> clientClosingListener, int numberOfLeases)
    {
        Preconditions.checkArgument(numberOfLeases > 0);
        PathUtils.validatePath(path);

        this.client = client;
        this.basePath = path;
        this.path = path + "/" + lockName;
        this.clientClosingListener = clientClosingListener;
        this.numberOfLeases = numberOfLeases;

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
                if ( CuratorEventType.isClosingType(event) )
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

    String internalAcquire() throws Exception
    {
        String      lockPath = internalAcquire(-1, null);
        if ( lockPath == null )
        {
            throw new IOException("Lost connection while trying to acquire lock");
        }
        return lockPath;
    }

    String internalAcquire(long time, TimeUnit unit) throws Exception
    {
        String      lockPath = internalLock(time, unit);

        if ( (lockPath != null) && (lockCount() == 1) )
        {
            client.addListener(listener);
        }
        return lockPath;
    }

    void internalRelease(String lockPath) throws Exception
    {
        client.removeListener(listener);

        client.delete().forPath(lockPath);
        clearLockData();
    }

    String getBasePath()
    {
        return basePath;
    }

    protected abstract T getInstance();

    protected abstract void setLockData(String ourPath);

    protected abstract void clearLockData();

    protected abstract int lockCount();

    protected String internalLock(long time, TimeUnit unit) throws Exception
    {
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

        boolean     doDelete = false;
        boolean     haveTheLock = false;
        try
        {
            while ( client.isStarted() && !haveTheLock )
            {
                List<String>    children = getSortedChildren(basePath);
                String          sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                int ourIndex = children.indexOf(sequenceNodeName);
                if ( ourIndex < 0 )
                {
                    throw new Exception("Sequential path not found: " + ourPath);
                }

                if ( ourIndex < numberOfLeases )
                {
                    // we have the lock
                    setLockData(ourPath);
                    haveTheLock = true;
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
                                    doDelete = true;    // timed out - delete our node
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
            doDelete = true;
            throw e;
        }
        finally
        {
            if ( doDelete )
            {
                client.delete().forPath(ourPath);
            }
        }

        return haveTheLock ? ourPath : null;
    }

    private void handleClosingEvent()
    {
        if ( clientClosingListener != null )
        {
            clientClosingListener.notifyClientClosing(getInstance(), client);
        }
        notifyFromWatcher();
    }

    private synchronized void notifyFromWatcher()
    {
        notifyAll();
    }

    private List<String> getSortedChildren(String path) throws Exception
    {
        List<String> children = client.getChildren().forPath(path);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort(sortedList);
        return sortedList;
    }
}
