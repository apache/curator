package org.apache.curator.x.rpc.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.rpc.idl.structs.RpcCuratorEvent;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import org.apache.curator.x.rpc.idl.services.CuratorProjectionService;

public class RpcBackgroundCallback implements BackgroundCallback
{
    private final CuratorProjection projection;
    private final CuratorProjectionService projectionService;

    public RpcBackgroundCallback(CuratorProjectionService projectionService, CuratorProjection projection)
    {
        this.projection = projection;
        this.projectionService = projectionService;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        projectionService.addEvent(projection, new RpcCuratorEvent(event));
    }
}
