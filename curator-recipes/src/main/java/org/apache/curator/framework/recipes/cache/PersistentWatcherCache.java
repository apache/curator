package org.apache.curator.framework.recipes.cache;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.nodes.PersistentWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class PersistentWatcherCache implements Closeable
{
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final PersistentWatcher persistentWatcher;
    private final ListenerContainer<PersistentWatcherCacheListener> listeners = new ListenerContainer<>();
    private final ConcurrentMap<String, ChildData> cache = new ConcurrentHashMap<>();
    private final AtomicReference<PersistentWatcherCacheFilter> cacheFilter = new AtomicReference<>(defaultCacheFilter);

    private static final PersistentWatcherCacheFilter defaultCacheFilter = new PersistentWatcherCacheFilter()
    {
        @Override
        public Action actionForPath(String path)
        {
            return Action.IGNORE;
        }
    };
    private final CuratorFramework client;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(final WatchedEvent event)
        {
            processEvent(event);
        }
    };

    public PersistentWatcherCache(CuratorFramework client, String basePath)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        persistentWatcher = new PersistentWatcher(client, basePath)
        {
            @Override
            protected void reset()
            {
                super.reset();
                refreshData();
            }
        };
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        persistentWatcher.getListenable().addListener(watcher);
        persistentWatcher.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            persistentWatcher.close();
        }
    }

    public Listenable<PersistentWatcherCacheListener> getListenable()
    {
        return listeners;
    }

    public void setCacheFilter(PersistentWatcherCacheFilter cacheFilter)
    {
        this.cacheFilter.set(Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null"));
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data.
     *
     * @return list of children and data
     */
    public Map<String, ChildData> getCurrentData()
    {
        return ImmutableMap.copyOf(cache);
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData getCurrentData(String fullPath)
    {
        return cache.get(fullPath);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     */
    public void clearDataBytes(String fullPath)
    {
        clearDataBytes(fullPath, -1);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath  the path of the node to clear
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     * @return true if the data was cleared
     */
    public boolean clearDataBytes(String fullPath, int ifVersion)
    {
        ChildData data = cache.get(fullPath);
        if ( data != null )
        {
            if ( (ifVersion < 0) || ((data.getStat() != null) && (ifVersion == data.getStat().getVersion())) )
            {
                if ( data.getData() != null )
                {
                    cache.replace(fullPath, data, new ChildData(data.getPath(), data.getStat(), null));
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Clears the current data without beginning a new query and without generating any events
     * for listeners.
     */
    public void clear()
    {
        cache.clear();
    }

    public void refreshData()
    {
        for ( String path : cache.keySet() )
        {
            PersistentWatcherCacheFilter.Action action = cacheFilter.get().actionForPath(path);
            if ( (action == PersistentWatcherCacheFilter.Action.GET_DATA_THEN_SAVE) || (action == PersistentWatcherCacheFilter.Action.SAVE_THEN_GET_DATA) )
            {
                getData(path);
            }
        }
    }

    private void processEvent(final WatchedEvent event)
    {
        switch ( event.getType() )
        {
            default:
            {
                // ignore
                break;
            }

            case NodeCreated:
            case NodeDataChanged:
            {
                updateNode(event.getPath());
                break;
            }

            case NodeDeleted:
            {
                deleteNode(event.getPath());
                break;
            }
        }
    }

    private void deleteNode(final String path)
    {
        if ( cache.remove(path) != null )
        {
            Function<PersistentWatcherCacheListener, Void> proc = new Function<PersistentWatcherCacheListener, Void>()
            {
                @Override
                public Void apply(PersistentWatcherCacheListener listener)
                {
                    listener.nodeDeleted(path);
                    return null;
                }
            };
            listeners.forEach(proc);
        }
    }

    private void updateNode(final String path)
    {
        boolean putAndCallListeners;
        boolean doGetData;
        switch ( cacheFilter.get().actionForPath(path) )
        {
            default:
            case IGNORE:
            {
                putAndCallListeners = doGetData = false;
                break;
            }

            case SAVE_ONLY:
            {
                putAndCallListeners = true;
                doGetData = false;
                break;
            }

            case SAVE_THEN_GET_DATA:
            {
                putAndCallListeners = doGetData = true;
                break;
            }

            case GET_DATA_THEN_SAVE:
            {
                doGetData = true;
                putAndCallListeners = false;
                break;
            }
        }

        if ( putAndCallListeners )
        {
            final ChildData oldData = cache.put(path, new ChildData(path, null, null));
            Function<PersistentWatcherCacheListener, Void> proc = new Function<PersistentWatcherCacheListener, Void>()
            {
                @Override
                public Void apply(PersistentWatcherCacheListener listener)
                {
                    if ( oldData == null )
                    {
                        listener.nodeCreated(path);
                    }
                    else
                    {
                        listener.nodeDataChanged(path);
                    }
                    return null;
                }
            };
            listeners.forEach(proc);
        }

        if ( doGetData )
        {
            getData(path);
        }
    }

    private void getData(String path)
    {
        try
        {
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, final CuratorEvent event) throws Exception
                {
                    if ( event.getType() == CuratorEventType.GET_DATA )
                    {
                        ChildData newData = new ChildData(event.getPath(), event.getStat(), event.getData());
                        ChildData oldData = cache.put(event.getPath(), newData);
                        if ( !newData.equals(oldData) )
                        {
                            Function<PersistentWatcherCacheListener, Void> proc = new Function<PersistentWatcherCacheListener, Void>()
                            {
                                @Override
                                public Void apply(PersistentWatcherCacheListener listener)
                                {
                                    listener.nodeDataAvailable(event.getPath());
                                    return null;
                                }
                            };
                            listeners.forEach(proc);
                        }
                    }
                }
            };
            client.getData().inBackground().forPath(path);
        }
        catch ( Exception e )
        {
            // TODO
        }
    }
}
