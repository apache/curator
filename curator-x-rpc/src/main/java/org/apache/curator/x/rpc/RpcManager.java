package org.apache.curator.x.rpc;

import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import java.util.Map;

public class RpcManager
{
    private final Map<String, CuratorEntry> projections = Maps.newConcurrentMap();

    public void add(String id, CuratorFramework client)
    {
        projections.put(id, new CuratorEntry(client));
    }

    public CuratorEntry get(String id)
    {
        CuratorEntry curatorEntry = projections.get(id);
        curatorEntry.updateLastAccess();
        return curatorEntry;
    }

    public CuratorEntry remove(String id)
    {
        return projections.remove(id);
    }
}
