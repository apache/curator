package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.curator.x.rpc.RpcManager;
import org.apache.curator.x.rpc.idl.projection.CuratorProjection;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@ThriftService("EventService")
public class EventService
{
    private final BlockingQueue<RpcCuratorEvent> events = Queues.newLinkedBlockingQueue();
    private final RpcManager rpcManager;
    private final int pingTimeMs;

    public EventService(RpcManager rpcManager, int pingTimeMs)
    {
        this.rpcManager = rpcManager;
        this.pingTimeMs = pingTimeMs;
    }

    public void addEvent(RpcCuratorEvent event)
    {
        events.offer(event);
    }

    @ThriftMethod
    public RpcCuratorEvent getNextEvent(List<CuratorProjection> projections) throws InterruptedException
    {
        if ( projections != null )
        {
            List<String> ids = Lists.transform
            (
                projections,
                new Function<CuratorProjection, String>()
                {
                    @Override
                    public String apply(CuratorProjection projection)
                    {
                        return projection.id;
                    }
                }
            );
            rpcManager.touch(ids);
        }
        RpcCuratorEvent event = events.poll(pingTimeMs, TimeUnit.MILLISECONDS);
        return (event != null) ? event : new RpcCuratorEvent();
    }
}
