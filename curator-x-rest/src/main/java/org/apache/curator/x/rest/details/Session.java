package org.apache.curator.x.rest.details;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Session implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, Entry> things = Maps.newConcurrentMap();
    private final AtomicLong lastUseMs = new AtomicLong(System.currentTimeMillis());

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

    public void updateLastUse()
    {
        lastUseMs.set(System.currentTimeMillis());
    }

    public long getLastUseMs()
    {
        return lastUseMs.get();
    }

    @Override
    public void close()
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
    }

    public String addThing(Object thing)
    {
        return addThing(thing, null);
    }

    public <T> String addThing(T thing, Closer<T> closer)
    {
        String id = SessionManager.newId();
        things.put(id, new Entry(thing, closer));
        return id;
    }

    public <T> T getThing(String id, Class<T> clazz)
    {
        Entry entry = things.get(id);
        return cast(clazz, entry);
    }

    public <T> T deleteThing(String id, Class<T> clazz)
    {
        Entry entry = things.remove(id);
        return cast(clazz, entry);
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
