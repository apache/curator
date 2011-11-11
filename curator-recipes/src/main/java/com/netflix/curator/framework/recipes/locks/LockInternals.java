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

import com.google.common.collect.Lists;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class LockInternals<T>
{
    private final CuratorFramework          client;
    private final String                    path;
    private final String                    basePath;
    private final ClientClosingListener<T>  clientClosingListener;
    private final CuratorListener           listener;
    private final EnsurePath                ensurePath;
    private final Watcher                   watcher;
    private final String                    lockName;
    private final T                         parent;
    private final Lock                      syncLock;
    private final Condition                 syncCondition;

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

    LockInternals(CuratorFramework client, String path, String lockName, T parent, ClientClosingListener<T> clientClosingListener)
    {
        this.lockName = lockName;
        this.parent = parent;
        PathUtils.validatePath(path);

        syncLock = new ReentrantLock();
        syncCondition = syncLock.newCondition();

        this.client = client;
        this.basePath = path;
        this.path = ZKPaths.makePath(path, lockName);
        this.clientClosingListener = clientClosingListener;

        ensurePath = client.newNamespaceAwareEnsurePath(basePath);

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
                    notifyListener(null);
                }
                else if ( event.getType() == CuratorEventType.WATCHED )
                {
                    if ( event.getWatchedEvent().getState() != Watcher.Event.KeeperState.SyncConnected )
                    {
                        notifyFromWatcher();
                    }
                }
            }

            @Override
            public void unhandledError(CuratorFramework client, Throwable e)
            {
                notifyListener(e);
            }
        };
    }

    void releaseLock(String lockPath) throws Exception
    {
        client.removeListener(listener);
        client.delete().forPath(lockPath);
    }

    String attemptLock(long time, TimeUnit unit, final SharedCount maxLeases) throws Exception
    {
        final long startMillis = System.currentTimeMillis();
        final Long millisToWait = (unit != null) ? unit.toMillis(time) : null;

        PathAndFlag     pathAndFlag = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<PathAndFlag>()
            {
                @Override
                public PathAndFlag call() throws Exception
                {
                    ensurePath.ensure(client.getZookeeperClient());

                    String      ourPath = client.create().withProtectedEphemeralSequential().forPath(path, new byte[0]);
                    boolean     hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath, maxLeases);
                    return new PathAndFlag(hasTheLock, ourPath);
                }
            }
        );

        if ( pathAndFlag.hasTheLock )
        {
            client.addListener(listener);
            return pathAndFlag.path;
        }

        return null;
    }

    Lock getSyncLock()
    {
        return syncLock;
    }

    Condition getSyncCondition()
    {
        return syncCondition;
    }

    private static class PathAndFlag
    {
        final boolean       hasTheLock;
        final String        path;

        private PathAndFlag(boolean hasTheLock, String path)
        {
            this.hasTheLock = hasTheLock;
            this.path = path;
        }
    }

    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath, SharedCount maxLeases) throws Exception
    {
        boolean     haveTheLock = false;
        boolean     doDelete = false;
        try
        {
            while ( client.isStarted() && !haveTheLock )
            {
                List<String>    children = getSortedChildren(basePath);
                String          sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                int             ourIndex = children.indexOf(sequenceNodeName);
                if ( ourIndex < 0 )
                {
                    client.getZookeeperClient().getLog().warn("Sequential path not found: " + ourPath);
                    notifyListener(null);
                    throw new KeeperException.ConnectionLossException(); // treat it as a kind of disconnection and just try again according to the retry policy
                }

                if ( ourIndex < maxLeases.getCount() )
                {
                    haveTheLock = true;
                }
                else
                {
                    String previousSequenceNodeName = children.get(ourIndex - 1);
                    String previousSequencePath = basePath + "/" + previousSequenceNodeName;
                    syncLock.lock();
                    try
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

                                syncCondition.await(millisToWait, TimeUnit.MILLISECONDS);
                            }
                            else
                            {
                                syncCondition.await();
                            }
                        }
                    }
                    finally
                    {
                        syncLock.unlock();
                    }
                    // else it may have been deleted (i.e. lock released). Try to acquire again
                }
            }
        }
        catch ( KeeperException e )
        {
            // ignore this and let the retry policy handle it
            throw e;
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
        return haveTheLock;
    }

    private void notifyListener(Throwable e)
    {
        if ( clientClosingListener != null )
        {
            if ( e != null )
            {
                clientClosingListener.unhandledError(client, e);
            }
            else
            {
                clientClosingListener.notifyClientClosing(parent, client);
            }
        }
        notifyFromWatcher();
    }

    private void notifyFromWatcher()
    {
        syncLock.lock();
        try
        {
            syncCondition.signalAll();
        }
        finally
        {
            syncLock.unlock();
        }
    }

    private String   fixForSorting(String str)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    private List<String> getSortedChildren(String path) throws Exception
    {
        List<String> children = client.getChildren().forPath(path);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
        (
            sortedList,
            new Comparator<String>()
            {
                @Override
                public int compare(String lhs, String rhs)
                {
                    return fixForSorting(lhs).compareTo(fixForSorting(rhs));
                }
            }
        );
        return sortedList;
    }
}
