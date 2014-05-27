package org.apache.curator.x.rpc;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.rpc.idl.event.RpcCuratorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorEntry implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final BlockingQueue<RpcCuratorEvent> events = Queues.newLinkedBlockingQueue();
    private final AtomicReference<State> state = new AtomicReference<State>(State.OPEN);
    private final Map<String, Entry> things = Maps.newConcurrentMap();

    private static class Entry
    {
        final Object thing;
        final Closer closer;

        private Entry(Object thing, Closer closer)
        {
            this.thing = thing;
            this.closer = closer;
        }
    }

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
            for ( Map.Entry<String, Entry> mapEntry : things.entrySet() )
            {
                Entry entry = mapEntry.getValue();
                if ( entry.closer != null )
                {
                    log.debug(String.format("Closing left over thing. Type: %s - Id: %s", entry.thing.getClass(), mapEntry.getKey()));
                    //noinspection unchecked
                    entry.closer.close(entry.thing);    // lack of generics is safe because addThing() is type-safe
                }
            }
            things.clear();

            client.close();
            events.clear();
        }
    }

    public RpcCuratorEvent pollForEvent(long maxWaitMs) throws InterruptedException
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

    public <T> String addThing(T thing, Closer<T> closer)
    {
        return addThing(UUID.randomUUID().toString(), thing, closer);
    }

    public <T> String addThing(String id, T thing, Closer<T> closer)
    {
        things.put(id, new Entry(thing, closer));
        return id;
    }

    public <T> T getThing(String id, Class<T> clazz)
    {
        Entry entry = (id != null) ? things.get(id) : null;
        return cast(clazz, entry);
    }

    public boolean closeThing(String id)
    {
        Entry entry = (id != null) ? things.remove(id) : null;
        if ( entry != null )
        {
            //noinspection unchecked
            entry.closer.close(entry.thing);
        }
        return false;
    }

    private <T> T cast(Class<T> clazz, Entry entry)
    {
        if ( entry != null )
        {
            return clazz.cast(entry.thing);
        }
        return null;
    }
}
