package org.apache.curator.x.rpc.idl;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import java.util.Map;
import java.util.UUID;

@ThriftService("curator")
public class CuratorProjectionService
{
    private final Map<String, CuratorFramework> projections = Maps.newConcurrentMap();

    @ThriftMethod
    public CuratorProjection newCuratorProjection(CuratorProjectionSpec spec)
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
        String id = UUID.randomUUID().toString();
        client.start();
        projections.put(id, client);
        return new CuratorProjection(id);
    }

    @ThriftMethod
    public void closeCuratorProjection(CuratorProjection projection)
    {
        CuratorFramework client = projections.remove(projection.getId());
        if ( client != null )
        {
            client.close();
        }
    }
}
