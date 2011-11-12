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

package com.netflix.curator.framework.recipes.shared;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.imps.ListenerEntry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages a shared integer. All clients watching the same path will have the up-to-date
 * value of the shared integer (considering ZK's normal consistency guarantees).
 */
public class SharedCount implements Closeable, SharedCountReader
{
    private final Map<SharedCountListener, ListenerEntry<SharedCountListener>> listeners = Maps.newConcurrentMap();
    private final CuratorFramework          client;
    private final String                    path;
    private final int                       seedValue;
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private final Watcher                   watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                if ( state.get() == State.STARTED )
                {
                    readCount();
                    notifyListeners();
                }
            }
            catch ( Exception e )
            {
                client.getZookeeperClient().getLog().error("From SharedCount process event", e);
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private volatile int        count;
    private volatile Stat       stat = new Stat();

    /**
     * @param client the client
     * @param path the shared path - i.e. where the shared count is stored
     * @param seedValue the initial value for the count if/f the path has not yet been created
     */
    public SharedCount(CuratorFramework client, String path, int seedValue)
    {
        this.client = client;
        this.path = path;
        this.seedValue = seedValue;
        count = seedValue;
    }

    @Override
    public int getCount()
    {
        return count;
    }

    /**
     * Change the shared count value irrespective of its previous state
     *
     * @param newCount new value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     setCount(int newCount) throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED);

        client.setData().forPath(path, toBytes(newCount));
        stat.setVersion(stat.getVersion() + 1);
        count = newCount;
    }

    /**
     * Changes the shared count only if its value has not changed since this client last
     * read it. If the count has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the count is not successful you can get the updated value
     * by calling {@link #getCount()}.
     *
     * @param newCount the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getCount()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean  trySetCount(int newCount) throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED);

        try
        {
            client.setData().withVersion(stat.getVersion()).forPath(path, toBytes(newCount));
            stat.setVersion(stat.getVersion() + 1);
            count = newCount;
            return true;
        }
        catch ( KeeperException.BadVersionException ignore )
        {
            // ignore
        }

        readCount();
        return false;
    }

    @Override
    public void     addListener(SharedCountListener listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void     addListener(SharedCountListener listener, Executor executor)
    {
        Preconditions.checkState(state.get() == State.STARTED);

        listeners.put(listener, new ListenerEntry<SharedCountListener>(listener, executor));
    }

    @Override
    public void     removeListener(SharedCountListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * The shared count must be started before it can be used. Call {@link #close()} when you are
     * finished with the shared count
     *
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED));

        try
        {
            client.create().creatingParentsIfNeeded().forPath(path, toBytes(seedValue));
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore
        }

        readCount();
    }

    @Override
    public void close() throws IOException
    {
        state.set(State.CLOSED);
        listeners.clear();
    }

    private synchronized void readCount() throws Exception
    {
        Stat    localStat = new Stat();
        byte[]  bytes = client.getData().storingStatIn(localStat).usingWatcher(watcher).forPath(path);
        stat = localStat;
        count = fromBytes(bytes);
    }

    private static byte[]   toBytes(int value)
    {
        byte[]      bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(value);
        return bytes;
    }

    private static int      fromBytes(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void notifyListeners()
    {
        for ( final ListenerEntry<SharedCountListener> entry : listeners.values() )
        {
            entry.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            entry.listener.countHasChanged(SharedCount.this, count);
                        }
                        catch ( Exception e )
                        {
                            client.getZookeeperClient().getLog().error("From SharedCount listener", e);
                        }
                    }
                }
            );
        }
    }
}
