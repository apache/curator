package org.apache.curator.framework.imps;

import com.google.common.collect.Maps;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

class WatcherRemovalManager
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFrameworkImpl client;
    private final Map<Watcher, String> entries = Maps.newConcurrentMap();

    WatcherRemovalManager(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    Watcher add(String path, Watcher watcher)
    {
        Watcher wrappedWatcher = new WrappedWatcher(entries, watcher);
        entries.put(wrappedWatcher, path);
        return wrappedWatcher;
    }

    void removeWatchers()
    {
        for ( Map.Entry<Watcher, String> entry : entries.entrySet() )
        {
            Watcher watcher = entry.getKey();
            String path = entry.getValue();
            try
            {
                log.debug("Removing watcher for path: " + path);
                RemoveWatchesBuilderImpl builder = new RemoveWatchesBuilderImpl(client);
                builder.prepInternalRemoval(watcher);
                builder.forPath(path);
            }
            catch ( Exception e )
            {
                String message = "Could not remove watcher for path: " + path;
                log.error(message);
            }
        }
    }

    private static class WrappedWatcher implements Watcher
    {
        private final Map<Watcher, String> entries;
        private final Watcher watcher;

        WrappedWatcher(Map<Watcher, String> entries, Watcher watcher)
        {
            this.entries = entries;
            this.watcher = watcher;
        }

        @Override
        public void process(WatchedEvent event)
        {
            entries.remove(this);
            watcher.process(event);
        }
    }
}
