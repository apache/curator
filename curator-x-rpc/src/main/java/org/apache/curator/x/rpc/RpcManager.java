package org.apache.curator.x.rpc;

import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RpcManager
{
    private final Map<String, CuratorEntry> projections = Maps.newConcurrentMap();

    private static class CuratorEntry
    {
        private final CuratorFramework client;
        private final AtomicLong lastAccessEpoch = new AtomicLong(0);

        private CuratorEntry(CuratorFramework client)
        {
            this.client = client;
            updateLastAccess();
        }

        void updateLastAccess()
        {
            lastAccessEpoch.set(System.currentTimeMillis());
        }
    }

    public void add(String id, CuratorFramework client)
    {
        projections.put(id, new CuratorEntry(client));
    }

    public void updateLastAccess(String id)
    {
        CuratorEntry entry = projections.get(id);
        if ( entry != null )
        {
            entry.updateLastAccess();
        }
    }

    public CuratorFramework getClient(String id)
    {
        CuratorEntry entry = projections.get(id);
        if ( entry != null )
        {
            entry.updateLastAccess();
            return entry.client;
        }
        return null;
    }

    public CuratorFramework removeClient(String id)
    {
        CuratorEntry entry = projections.remove(id);
        return (entry != null) ? entry.client : null;
    }
}
