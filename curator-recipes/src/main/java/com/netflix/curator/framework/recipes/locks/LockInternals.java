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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class LockInternals
{
    private final CuratorFramework                  client;
    private final String                            path;
    private final String                            basePath;
    private final CuratorListener                   listener;
    private final EnsurePath                        ensurePath;
    private final Watcher                           watcher;
    private final LockInternalsDriver               driver;
    private final String                            lockName;
    private final AtomicReference<RevocationSpec>   revocable = new AtomicReference<RevocationSpec>(null);
    private final Watcher                           revocableWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            if ( event.getType() == Event.EventType.NodeDataChanged )
            {
                try
                {
                    checkRevocableWatcher(event.getPath());
                }
                catch ( Exception e )
                {
                    client.getZookeeperClient().getLog().error("From RevocableWatcher check", e);
                }
            }
        }
    };

    private volatile int    maxLeases;

    static final byte[]             REVOKE_MESSAGE = "__REVOKE__".getBytes();

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

    LockInternals(CuratorFramework client, LockInternalsDriver driver, String path, String lockName, int maxLeases)
    {
        this.driver = driver;
        this.lockName = lockName;
        this.maxLeases = maxLeases;
        PathUtils.validatePath(path);

        this.client = client;
        this.basePath = path;
        this.path = ZKPaths.makePath(path, lockName);

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
                if ( event.getType() == CuratorEventType.WATCHED )
                {
                    if ( event.getWatchedEvent().getState() != Watcher.Event.KeeperState.SyncConnected )
                    {
                        notifyFromWatcher();
                    }
                }
            }
        };
    }

    synchronized void setMaxLeases(int maxLeases)
    {
        this.maxLeases = maxLeases;
        notifyAll();
    }

    void makeRevocable(RevocationSpec entry)
    {
        revocable.set(entry);
    }

    void releaseLock(String lockPath) throws Exception
    {
        revocable.set(null);
        client.getCuratorListenable().removeListener(listener);
        client.delete().forPath(lockPath);
    }

    List<String> getSortedChildren() throws Exception
    {
        List<String> children = client.getChildren().forPath(basePath);
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
        (
            sortedList,
            new Comparator<String>()
            {
                @Override
                public int compare(String lhs, String rhs)
                {
                    return driver.fixForSorting(lhs, lockName).compareTo(driver.fixForSorting(rhs, lockName));
                }
            }
        );
        return sortedList;
    }

    String attemptLock(long time, TimeUnit unit, byte[] lockNodeBytes) throws Exception
    {
        final long      startMillis = System.currentTimeMillis();
        final Long      millisToWait = (unit != null) ? unit.toMillis(time) : null;
        final byte[]    localLockNodeBytes = (revocable.get() != null) ? new byte[0] : lockNodeBytes;

        PathAndFlag     pathAndFlag = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<PathAndFlag>()
            {
                @Override
                public PathAndFlag call() throws Exception
                {
                    ensurePath.ensure(client.getZookeeperClient());

                    String      ourPath;
                    if ( localLockNodeBytes != null )
                    {
                        ourPath = client.create().withProtectedEphemeralSequential().forPath(path, localLockNodeBytes);
                    }
                    else
                    {
                        ourPath = client.create().withProtectedEphemeralSequential().forPath(path);
                    }
                    boolean     hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath);
                    return new PathAndFlag(hasTheLock, ourPath);
                }
            }
        );

        if ( pathAndFlag.hasTheLock )
        {
            client.getCuratorListenable().addListener(listener);
            return pathAndFlag.path;
        }

        return null;
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

    private void checkRevocableWatcher(String path) throws Exception
    {
        RevocationSpec  entry = revocable.get();
        if ( entry != null )
        {
            try
            {
                byte[]      bytes = client.getData().usingWatcher(revocableWatcher).forPath(path);
                if ( Arrays.equals(bytes, REVOKE_MESSAGE) )
                {
                    entry.getExecutor().execute(entry.getRunnable());
                }
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
        }
    }

    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception
    {
        if ( revocable.get() != null )
        {
            client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
        }

        boolean     haveTheLock = false;
        boolean     doDelete = false;
        try
        {
            while ( client.isStarted() && !haveTheLock )
            {
                List<String>        children = getSortedChildren();
                String              sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash

                PredicateResults    predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                if ( predicateResults.getsTheLock() )
                {
                    haveTheLock = true;
                }
                else
                {
                    String  previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();
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

    private synchronized void notifyFromWatcher()
    {
        notifyAll();
    }
}
