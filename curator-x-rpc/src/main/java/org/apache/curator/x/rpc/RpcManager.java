package org.apache.curator.x.rpc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class RpcManager implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Cache<String, CuratorEntry> cache;

    public RpcManager(long expirationMs)
    {
        RemovalListener<String, CuratorEntry> listener = new RemovalListener<String, CuratorEntry>()
        {
            @SuppressWarnings("NullableProblems")
            @Override
            public void onRemoval(RemovalNotification<String, CuratorEntry> notification)
            {
                if ( notification != null )
                {
                    log.debug(String.format("Entry being removed. id (%s), reason (%s)", notification.getKey(), notification.getCause()));

                    CuratorEntry entry = notification.getValue();
                    if ( entry != null )
                    {
                        entry.close();
                    }
                }
            }
        };
        cache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(expirationMs, TimeUnit.MILLISECONDS)
            .removalListener(listener)
            .build();
    }

    @Override
    public void close()
    {
        cache.invalidateAll();
        cache.cleanUp();
    }

    public void add(String id, CuratorFramework client)
    {
        cache.put(id, new CuratorEntry(client));
    }

    public CuratorEntry get(String id)
    {
        return cache.getIfPresent(id);
    }

    public CuratorEntry remove(String id)
    {
        return cache.asMap().remove(id);
    }
}
