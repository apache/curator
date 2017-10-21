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
package org.apache.curator.framework.recipes.shared;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionState;
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

    protected SharedCount(CuratorFramework client, String path, SharedValue sv)
    {
        sharedValue = sv;
    }

    @Override
    public int getCount()
    {
        return fromBytes(sharedValue.getValue());
    }

    @Override
    public VersionedValue<Integer> getVersionedValue()
    {
        VersionedValue<byte[]> localValue = sharedValue.getVersionedValue();
        return new VersionedValue<Integer>(localValue.getVersion(), fromBytes(localValue.getValue()));
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
     * @deprecated use {@link #trySetCount(VersionedValue, int)} for stronger atomicity
     * guarantees. Even if this object's internal state is up-to-date, the caller has no way to
     * ensure that they've read the most recently seen count.
     *
     * @param newCount the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getCount()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    @Deprecated
    public boolean  trySetCount(int newCount) throws Exception
    {
        return sharedValue.trySetValue(toBytes(newCount));
    }

    /**
     * Changes the shared count only if its value has not changed since the version specified by
     * newCount. If the count has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the count is not successful you can get the updated value
     * by calling {@link #getCount()}.
     *
     * @param newCount the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getCount()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean  trySetCount(VersionedValue<Integer> previous, int newCount) throws Exception
    {
        VersionedValue<byte[]> previousCopy = new VersionedValue<byte[]>(previous.getVersion(), toBytes(previous.getValue()));
        return sharedValue.trySetValue(previousCopy, toBytes(newCount));
    }

    @Override
    public void     addListener(SharedCountListener listener)
    {
        addListener(listener, MoreExecutors.directExecutor());
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
        SharedValueListener valueListener = listeners.remove(listener);
        if(valueListener != null) {
            sharedValue.getListenable().removeListener(valueListener);
        }
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

    @VisibleForTesting
    static byte[]   toBytes(int value)
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
