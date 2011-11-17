/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.recipes.shared;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.state.ConnectionState;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Manages a shared integer. All clients watching the same path will have the up-to-date
 * value of the shared integer (considering ZK's normal consistency guarantees).
 */
public class SharedCount implements Closeable, SharedCountReader, Listenable<SharedCountListener>
{
    private final Map<SharedCountListener, SharedValueListener> listeners = Maps.newConcurrentMap();
    private final SharedValue           sharedValue;

    /**
     * @param client the client
     * @param path the shared path - i.e. where the shared count is stored
     * @param seedValue the initial value for the count if/f the path has not yet been created
     */
    public SharedCount(CuratorFramework client, String path, int seedValue)
    {
        sharedValue = new SharedValue(client, path, toBytes(seedValue));
    }

    @Override
    public int getCount()
    {
        return fromBytes(sharedValue.getValue());
    }

    /**
     * Change the shared count value irrespective of its previous state
     *
     * @param newCount new value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     setCount(int newCount) throws Exception
    {
        sharedValue.setValue(toBytes(newCount));
    }

    /**
     * Changes the shared count only if its value has not changed since this client last
     * read it. If the count has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the count is not successful you can get the updated value
     * by calling {@link #getCount()}.
     *
     * @param newCount the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getCount()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean  trySetCount(int newCount) throws Exception
    {
        return sharedValue.trySetValue(toBytes(newCount));
    }

    @Override
    public void     addListener(SharedCountListener listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void     addListener(final SharedCountListener listener, Executor executor)
    {
        SharedValueListener     valueListener = new SharedValueListener()
        {
            @Override
            public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception
            {
                listener.countHasChanged(SharedCount.this, fromBytes(newValue));
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                listener.stateChanged(client, newState);
            }
        };
        sharedValue.getListenable().addListener(valueListener, executor);
        listeners.put(listener, valueListener);
    }

    @Override
    public void     removeListener(SharedCountListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * The shared count must be started before it can be used. Call {@link #close()} when you are
     * finished with the shared count
     *
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     start() throws Exception
    {
        sharedValue.start();
    }

    @Override
    public void close() throws IOException
    {
        sharedValue.close();
    }

    private static byte[]   toBytes(int value)
    {
        byte[]      bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(value);
        return bytes;
    }

    private static int      fromBytes(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes).getInt();
    }
}
