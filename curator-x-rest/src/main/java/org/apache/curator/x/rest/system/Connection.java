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

package org.apache.curator.x.rest.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.rest.entity.ConnectionStateEntity;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Connection implements Closeable, ConnectionStateListener
{
    private final CuratorFramework client;
    private final AtomicLong lastUseMs = new AtomicLong(System.currentTimeMillis());
    private final Map<ThingKey, Object> things = Maps.newConcurrentMap();
    private final Object stateLock = new Object();
    private long stateCount = 0;  // guarded by stateLock

    public Connection(CuratorFramework client)
    {
        this.client = client;
        client.getConnectionStateListenable().addListener(this);
    }

    @Override
    public void close()
    {
        client.close();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        synchronized(stateLock)
        {
            ++stateCount;
            stateLock.notifyAll();
        }
    }

    public ConnectionStateEntity getState()
    {
        synchronized(stateLock)
        {
            return new ConnectionStateEntity(client.getState().name(), stateCount);
        }
    }

    public void blockUntilStateChange(long expectedStateCount) throws InterruptedException
    {
        synchronized(stateLock)
        {
            if ( expectedStateCount < 0 )
            {
                expectedStateCount = stateCount;
            }

            while ( stateCount == expectedStateCount )
            {
                stateLock.wait();
            }
        }
    }

    public void updateUse()
    {
        for ( Map.Entry<ThingKey, Object> entry : things.entrySet() )
        {
            //noinspection unchecked
            entry.getKey().getType().closeFor(entry.getValue());
        }
        lastUseMs.set(System.currentTimeMillis());
    }

    public CuratorFramework getClient()
    {
        return client;
    }

    public long getLastUseMs()
    {
        return lastUseMs.get();
    }

    public <T> void putThing(ThingKey<T> key, T thing)
    {
        thing = Preconditions.checkNotNull(thing, "thing cannot be null");
        things.put(key, thing);
    }

    public <T> T getThing(ThingKey<T> key)
    {
        Object o = things.get(key);
        if ( o != null )
        {
            return key.getType().getThingClass().cast(o);
        }
        return null;
    }

    public <T> T removeThing(ThingKey<T> key)
    {
        Object o = things.remove(key);
        if ( o != null )
        {
            return key.getType().getThingClass().cast(o);
        }
        return null;
    }
}
