/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.x.rest.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Session implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, Entry> things = Maps.newConcurrentMap();
    private final BlockingQueue<StatusMessage> messages = Queues.newLinkedBlockingQueue();

    private static class Entry
    {
        final Object thing;
        final Closer closer;
        final AtomicLong lastUseMs = new AtomicLong(System.currentTimeMillis());

        private Entry(Object thing, Closer closer)
        {
            this.thing = thing;
            this.closer = closer;
        }
    }

    @Override
    public synchronized void close()
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
    }

    public synchronized void checkExpiredThings(long sessionLengthMs)
    {
        for ( Map.Entry<String, Entry> mapEntry : things.entrySet() )
        {
            Entry entry = mapEntry.getValue();
            long elapsedSinceLastUse = System.currentTimeMillis() - entry.lastUseMs.get();
            if ( elapsedSinceLastUse > sessionLengthMs )
            {
                String id = mapEntry.getKey();
                pushMessage(new StatusMessage(Constants.EXPIRED, id, "expired", entry.thing.getClass().getName()));
                log.warn(String.format("Expiring object. Elapsed time: %d, id: %s, Class: %s", elapsedSinceLastUse, id, entry.thing.getClass().getName()));

                things.remove(id);
                if ( entry.closer != null )
                {
                    //noinspection unchecked
                    entry.closer.close(entry.thing);    // lack of generics is safe because addThing() is type-safe
                }
            }
        }
    }

    void pushMessage(StatusMessage message)
    {
        messages.add(message);
    }

    Collection<StatusMessage> drainMessages()
    {
        List<StatusMessage> localMessages = Lists.newArrayList();
        messages.drainTo(localMessages);
        return localMessages;
    }

    boolean updateThingLastUse(String id)
    {
        Entry entry = things.get(id);
        if ( entry != null )
        {
            entry.lastUseMs.set(System.currentTimeMillis());
            return true;
        }
        return false;
    }

    <T> String addThing(T thing, Closer<T> closer)
    {
        return addThing(Constants.newId(), thing, closer);
    }

    <T> String addThing(String id, T thing, Closer<T> closer)
    {
        things.put(id, new Entry(thing, closer));
        return id;
    }

    <T> T getThing(String id, Class<T> clazz)
    {
        Entry entry = things.get(id);
        return cast(clazz, entry);
    }

    <T> T deleteThing(String id, Class<T> clazz)
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
