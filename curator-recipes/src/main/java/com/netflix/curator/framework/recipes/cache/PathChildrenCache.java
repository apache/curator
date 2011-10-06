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
package com.netflix.curator.framework.recipes.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.io.IOException;
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
    private final CuratorFramework          client;
    private final String                    path;
    private final PathChildrenCacheMode     mode;
    private final ExecutorService           executorService;

    private static final ChildData          existingDataMarker = new ChildData(null, null, null);

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

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     */
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode)
    {
        this(client, path, mode, Executors.defaultThreadFactory());
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode, ThreadFactory threadFactory)
    {
        this.client = client;
        this.path = path;
        this.mode = mode;
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
                    Thread.currentThread().setName("PathChildrenCache " + Thread.currentThread().getName());
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
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData>      getCurrentData()
    {
        return ImmutableList.copyOf(Sets.<ChildData>newTreeSet(currentData.values()));
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData            getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        refresh();
    }

    private void refresh() throws Exception
    {
        incomingData.clear();
        client.getChildren().usingWatcher(watcher).inBackground().forPath(path);
        listenerEvents.offer(new EventEntry(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.RESET, null)));
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
                processChildren(event.getChildren());
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
            case NodeDataChanged:
            {
                processDataChanged(watchedEvent.getPath());
                break;
            }

            default:
            {
                if ( watchedEvent.getState() == Watcher.Event.KeeperState.Expired )
                {
                    System.out.println();
                }
                refresh();
                break;
            }
        }
    }

    private void processDataChanged(String path) throws Exception
    {
        addIncomingPath(path);
    }

    private void        checkSetCurrent()
    {
        for ( Map.Entry<String, ChildData> entry : incomingData.entrySet() )
        {
            String          path = entry.getKey();
            ChildData       data = entry.getValue();

            if ( data.isComplete(mode) )
            {
                boolean     isNew = (currentData.put(data.getPath(), data) == null);
                incomingData.remove(path);

                listenerEvents.offer(new EventEntry(new PathChildrenCacheEvent(isNew ? PathChildrenCacheEvent.Type.CHILD_ADDED : PathChildrenCacheEvent.Type.CHILD_UPDATED, data)));
            }
            else if ( isTheExistingDataMarker(data) )
            {
                ChildData       removedData = currentData.remove(path);
                incomingData.remove(path);

                listenerEvents.offer(new EventEntry(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, removedData)));
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

    private void processChildren(List<String> children) throws Exception
    {
        for ( String path : currentData.keySet() )
        {
            incomingData.put(path, existingDataMarker);
        }

        for ( String child : children )
        {
            String      actualPath = ZKPaths.makePath(path, child);
            addIncomingPath(actualPath);
        }

        checkSetCurrent();
    }

    private void addIncomingPath(String actualPath) throws Exception
    {
        incomingData.put(actualPath, new ChildData(actualPath, null, null));

        switch ( mode )
        {
            case CACHE_DATA_AND_STAT:
            {
                client.checkExists().inBackground().forPath(actualPath);    // to get the stat
                client.getData().usingWatcher(watcher).inBackground().forPath(actualPath);  // watcher checks for data change
                break;
            }

            case CACHE_DATA:
            {
                client.getData().usingWatcher(watcher).inBackground().forPath(actualPath);  // watcher checks for data change
                break;
            }

            case CACHE_PATHS_ONLY:
            {
                // do nothing
                break;
            }
        }
    }

    private static boolean isTheExistingDataMarker(ChildData data)
    {
        return data == existingDataMarker;
    }
}
