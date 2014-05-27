package org.apache.curator.x.rpc;

import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.rpc.idl.event.RpcCuratorEvent;
import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorEntry implements Closeable
{
    private final CuratorFramework client;
    private final BlockingQueue<RpcCuratorEvent> events = Queues.newLinkedBlockingQueue();
    private final AtomicReference<State> state = new AtomicReference<State>(State.OPEN);

    private enum State
    {
        OPEN,
        CLOSED
    }

    public CuratorEntry(CuratorFramework client)
    {
        this.client = client;
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.OPEN, State.CLOSED) )
        {
            client.close();
            events.clear();
        }
    }

    public RpcCuratorEvent pollForEvent(int maxWaitMs) throws InterruptedException
    {
        if ( state.get() == State.OPEN )
        {
            return events.poll(maxWaitMs, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    public void addEvent(RpcCuratorEvent event)
    {
        if ( state.get() == State.OPEN )
        {
            events.offer(event);
        }
    }

    public CuratorFramework getClient()
    {
        return (state.get() == State.OPEN) ? client : null;
    }
}
