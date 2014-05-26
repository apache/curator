package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.collect.Queues;
import java.util.concurrent.BlockingQueue;

@ThriftService("CuratorEventService")
public class CuratorEventService
{
    private final BlockingQueue<CuratorRpcEvent> events = Queues.newLinkedBlockingQueue();

    @ThriftMethod
    public CuratorRpcEvent getNextEvent() throws InterruptedException
    {
        return events.take();
    }

    public void addEvent(CuratorRpcEvent event)
    {
        events.offer(event);
    }
}
