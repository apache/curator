package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.collect.Queues;
import java.util.concurrent.BlockingQueue;

@ThriftService("EventService")
public class EventService
{
    private final BlockingQueue<RpcCuratorEvent> events = Queues.newLinkedBlockingQueue();

    public void addEvent(RpcCuratorEvent event)
    {
        events.offer(event);
    }

    @ThriftMethod
    public RpcCuratorEvent getNextEvent() throws InterruptedException
    {
        return events.take();
    }
}
