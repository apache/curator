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
package com.netflix.curator.framework.recipes.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 *
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class PathChildrenCache implements Closeable
{
    private final CuratorFramework client;
    private final String                    path;
    private final ExecutorService           executorService;

    private final BlockingQueue<EventEntry>                         listenerEvents = new LinkedBlockingQueue<EventEntry>();
    private final Map<PathChildrenCacheListener, ListenerEntry>     listeners = Maps.newConcurrentMap();
    private final Map<String, ChildData>                            currentData = Maps.newConcurrentMap();
    private final Map<String, ChildData>                            incomingData = Maps.newConcurrentMap();
    private final Watcher                                           watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                processWatched(event);
            }
            catch ( Exception e )
            {
                listenerEvents.offer(new EventEntry(e));
            }
        }
    };

    private final CuratorListener curatorListener = new CuratorListener()
    {
        @Override
        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
        {
            processEvent(event);
        }

        @Override
        public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
        {
        }
    };

    private static class ListenerEntry
    {
        final PathChildrenCacheListener     listener;
        final Executor                      executor;

        private ListenerEntry(PathChildrenCacheListener listener, Executor executor)
        {
            this.listener = listener;
            this.executor = executor;
        }
    }

    private static class EventEntry
    {
        final PathChildrenCacheEvent  event;
        final Exception               exception;

        private EventEntry()
        {
            this.event = null;
            this.exception = null;
        }

        private EventEntry(PathChildrenCacheEvent event)
        {
            this.event = event;
            this.exception = null;
        }

        private EventEntry(Exception exception)
        {
            this.event = null;
            this.exception = exception;
        }
    }

    private static final int                EXPIRE_INCOMING_TIME_MS = 5 * 60 * 60 * 1000;   // 5 minutes

    /**
     * @param client the client
     * @param path path to watch
     */
    public PathChildrenCache(CuratorFramework client, String path)
    {
        this(client, path, Executors.defaultThreadFactory());
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, ThreadFactory threadFactory)
    {
        this.client = client;
        this.path = path;
        executorService = Executors.newFixedThreadPool(1, threadFactory);
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        Preconditions.checkArgument(!executorService.isShutdown());

        client.addListener(curatorListener, MoreExecutors.sameThreadExecutor());
        executorService.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    listenerLoop();
                    return null;
                }
            }
        );

        refresh();
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        Preconditions.checkArgument(!executorService.isShutdown());

        client.removeListener(curatorListener);
        executorService.shutdownNow();
    }

    /**
     * Add a change listener
     *
     * @param listener the listener
     */
    public void addListener(PathChildrenCacheListener listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    /**
     * Add a change listener
     *
     * @param listener the listener
     * @param executor executor to use when calling the listener
     */
    public void addListener(PathChildrenCacheListener listener, Executor executor)
    {
        listeners.put(listener, new ListenerEntry(listener, executor));
    }

    /**
     * Remove the given listener
     *
     * @param listener listener to remove
     */
    public void removeListener(PathChildrenCacheListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * Return a copy of the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData>      getCurrentData()
    {
        return ImmutableList.copyOf(Sets.<ChildData>newTreeSet(currentData.values()));
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        incomingData.clear();
        refresh();
    }

    private void refresh() throws Exception
    {
        client.getChildren().usingWatcher(watcher).inBackground().forPath(path);
    }

    private void listenerLoop()
    {
        boolean     unexpectedClose = false;
        while ( !Thread.currentThread().isInterrupted() )
        {
            try
            {
                final EventEntry event = listenerEvents.take();
                if ( (event.event == null) && (event.exception == null) )
                {
                    // special case - unexpected close
                    unexpectedClose = true;
                    break;
                }

                for ( final ListenerEntry listener : listeners.values() )
                {
                    listener.executor.execute
                    (
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    if ( event.event != null )
                                    {
                                        listener.listener.childEvent(client, event.event);
                                    }
                                    else
                                    {
                                        listener.listener.handleException(client, event.exception);
                                    }
                                }
                                catch ( Exception e )
                                {
                                    listenerEvents.offer(new EventEntry(e));
                                }
                            }
                        }
                    );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if ( unexpectedClose )
        {
            handleUnexpectedClose();
        }
    }

    private void handleUnexpectedClose()
    {
        Closeables.closeQuietly(this);
        for ( final ListenerEntry listener : listeners.values() )
        {
            listener.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        listener.listener.notifyClientClosing(client);
                    }
                }
            );
        }
    }

    private void processEvent(CuratorEvent event) throws Exception
    {
        switch ( event.getType() )
        {
            case CHILDREN:
            {
                processChildren(event.getChildren(), true);
                break;
            }

            case GET_DATA:
            {
                processGetData(event.getPath(), event.getData());
                break;
            }

            case EXISTS:
            {
                processExists(event.getPath(), event.getStat());
                break;
            }

            case WATCHED:
            {
                processWatched(event.getWatchedEvent());
                break;
            }

            case CLOSING:
            {
                listenerEvents.offer(new EventEntry());
                break;
            }

            default:
            {
                // do nothing
                break;
            }
        }
    }

    private void processWatched(WatchedEvent watchedEvent) throws Exception
    {
        switch ( watchedEvent.getType() )
        {
            case NodeCreated:
            case NodeDataChanged:
            {
                processNodeCreated(ZKPaths.getNodeFromPath(watchedEvent.getPath()));
                break;
            }

            case NodeDeleted:
            {
                processNodeDeleted(watchedEvent.getPath());
                break;
            }

            case NodeChildrenChanged:
            {
                refresh();
                break;
            }
        }
    }

    private void processNodeCreated(String path) throws Exception
    {
        List<String>        l = Lists.newArrayList(path);
        processChildren(l, false);
    }

    private void processNodeDeleted(String path)
    {
        ChildData       oldData = currentData.remove(path);
        incomingData.remove(path);

        if ( oldData != null )
        {
            listenerEvents.offer(new EventEntry(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, oldData)));
        }
    }

    private void        checkSetCurrent()
    {
        Iterator<ChildData>     iterator = incomingData.values().iterator();
        while ( iterator.hasNext() )
        {
            ChildData       data = iterator.next();
            if ( data.isComplete() )
            {
                boolean     isNew = (currentData.put(data.getPath(), data) == null);
                iterator.remove();

                listenerEvents.offer(new EventEntry(new PathChildrenCacheEvent(isNew ? PathChildrenCacheEvent.Type.CHILD_ADDED : PathChildrenCacheEvent.Type.CHILD_UPDATED, data)));
            }
            else
            {
                long        age = System.currentTimeMillis() - data.getThisObjectCreationTimeMs();
                if ( age >= EXPIRE_INCOMING_TIME_MS )
                {
                    iterator.remove();
                }
            }
        }
    }

    private void processExists(String path, Stat stat)
    {
        ChildData   data = incomingData.get(path);
        if ( data != null )
        {
            data = data.setStat(stat);
            incomingData.put(path, data);
        }

        checkSetCurrent();
    }

    private void processGetData(String path, byte[] bytes)
    {
        ChildData   data = incomingData.get(path);
        if ( data != null )
        {
            data = data.setData(bytes);
            incomingData.put(path, data);
        }

        checkSetCurrent();
    }

    private void processChildren(List<String> children, boolean doRetain) throws Exception
    {
        for ( String child : children )
        {
            String actualPath = ZKPaths.makePath(path, child);
            incomingData.put(actualPath, new ChildData(actualPath, null, null));
        }
        if ( doRetain )
        {
            currentData.keySet().retainAll(incomingData.keySet());
        }

        for ( String child : children )
        {
            String actualPath = ZKPaths.makePath(path, child);
            client.getData().usingWatcher(watcher).inBackground().forPath(actualPath);
            client.checkExists().usingWatcher(watcher).inBackground().forPath(actualPath);
        }
    }
}
