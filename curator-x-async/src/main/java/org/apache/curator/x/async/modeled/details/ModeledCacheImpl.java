package org.apache.curator.x.async.modeled.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.ModeledCache;
import org.apache.curator.x.async.modeled.cached.ModeledCacheEventType;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.cached.ModeledCachedNode;
import org.apache.zookeeper.data.Stat;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class ModeledCacheImpl<T> implements TreeCacheListener, ModeledCache<T>
{
    private final TreeCache cache;
    private final Map<ZPath, Entry<T>> entries = new ConcurrentHashMap<>();
    private final ModelSerializer<T> serializer;
    private final ListenerContainer<ModeledCacheListener<T>> listenerContainer = new ListenerContainer<>();

    private static final class Entry<T>
    {
        final Stat stat;
        final T model;

        Entry(Stat stat, T model)
        {
            this.stat = stat;
            this.model = model;
        }
    }

    ModeledCacheImpl(CuratorFramework client, ZPath path, ModelSerializer<T> serializer, boolean compressed)
    {
        this.serializer = serializer;
        cache = TreeCache.newBuilder(client, path.fullPath())
            .setCacheData(false)
            .setDataIsCompressed(compressed)
            .build();
    }

    public void start()
    {
        try
        {
            cache.start();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        cache.close();
        entries.clear();
    }

    @Override
    public Optional<ModeledCachedNode<T>> getCurrentData(ZPath path)
    {
        Entry<T> entry = entries.remove(path);
        if ( entry != null )
        {
            return Optional.of(new InternalCachedNode<>(path, entry));
        }
        return Optional.empty();
    }

    @Override
    public Map<ZPath, ModeledCachedNode<T>> getCurrentChildren(ZPath path)
    {
        return entries.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(path))
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), new InternalCachedNode<>(entry.getKey(), entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Listenable<ModeledCacheListener<T>> getListenable()
    {
        return listenerContainer;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
    {
        switch ( event.getType() )
        {
        case NODE_ADDED:
        case NODE_UPDATED:
        {
            ZPath path = ZPath.from(event.toString());
            T model = serializer.deserialize(event.getData().getData());
            entries.put(path, new Entry<>(event.getData().getStat(), model));
            ModeledCacheEventType type = (event.getType() == TreeCacheEvent.Type.NODE_ADDED) ? ModeledCacheEventType.NODE_ADDED : ModeledCacheEventType.NODE_UPDATED;
            accept(type, path, event.getData().getStat(), model);
            break;
        }

        case NODE_REMOVED:
        {
            ZPath path = ZPath.from(event.toString());
            Entry<T> entry = entries.remove(path);
            T model = (entry != null) ? entry.model : serializer.deserialize(event.getData().getData());
            Stat stat = (entry != null) ? entry.stat : event.getData().getStat();
            accept(ModeledCacheEventType.NODE_REMOVED, path, stat, model);
            break;
        }

        case INITIALIZED:
        {
            listenerContainer.forEach(l -> {
                l.initialized();
                return null;
            });
            break;
        }

        default:
            // ignore
            break;
        }
    }

    private void accept(ModeledCacheEventType type, ZPath path, Stat stat, T model)
    {
        listenerContainer.forEach(l -> {
            l.accept(type, path, stat, model);
            return null;
        });
    }

    private static class InternalCachedNode<U> implements ModeledCachedNode<U>
    {
        private final ZPath path;
        private final Entry<U> entry;

        private InternalCachedNode(ZPath path, Entry<U> entry)
        {
            this.path = path;
            this.entry = entry;
        }

        @Override
        public ZPath getPath()
        {
            return path;
        }

        @Override
        public Stat getStat()
        {
            return entry.stat;
        }

        @Override
        public U getModel()
        {
            return entry.model;
        }
    }
}
