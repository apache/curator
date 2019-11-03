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
package org.apache.curator.v2.recipes.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.v2.CuratorFrameworkV2;
import org.apache.curator.v2.recipes.watch.PersistentWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.curator.v2.recipes.cache.CuratorCacheListener.Type.*;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.KeeperException.Code.OK;

class CuratorCacheImpl implements CuratorCache
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final PersistentWatcher persistentWatcher;
    private final CuratorFrameworkV2 client;
    private final CuratorCacheStorage storage;
    private final String path;
    private final boolean recursive;
    private final boolean compressedData;
    private final boolean clearOnClose;
    private final StandardListenerManager<CuratorCacheListener> listenerManager = StandardListenerManager.standard();
    private final Consumer<Exception> exceptionHandler;
    private final Executor executor;

    private volatile AtomicLong outstandingOps;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    CuratorCacheImpl(CuratorFrameworkV2 client, CuratorCacheStorage storage, String path, Options[] optionsArg, Executor executor, Consumer<Exception> exceptionHandler)
    {
        Set<Options> options = (optionsArg != null) ? Sets.newHashSet(optionsArg) : Collections.emptySet();
        this.client = client;
        this.storage = (storage != null) ? storage : CuratorCacheStorage.standard();
        this.path = path;
        recursive = !options.contains(Options.SINGLE_NODE_CACHE);
        compressedData = options.contains(Options.COMPRESSED_DATA);
        clearOnClose = !options.contains(Options.DO_NOT_CLEAR_ON_CLOSE);
        persistentWatcher = new PersistentWatcher(client, path, recursive);
        persistentWatcher.getListenable().addListener(this::processEvent);
        persistentWatcher.getResetListenable().addListener(this::rebuild);
        this.exceptionHandler = (exceptionHandler != null) ? exceptionHandler : e -> log.error("CuratorCache error", e);
        this.executor = (executor != null) ? executor : client::runSafe;
    }

    @Override
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        outstandingOps = new AtomicLong(0);
        persistentWatcher.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            persistentWatcher.close();
            if ( clearOnClose )
            {
                storage.clear();
            }
        }
    }

    @Override
    public CuratorCacheStorage storage()
    {
        return storage;
    }

    @Override
    public Listenable<CuratorCacheListener> listenable()
    {
        return listenerManager;
    }

    private void rebuild()
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        nodeChanged(path);
        storage.stream()
            .map(ChildData::getPath)
            .filter(p -> !p.equals(path))
            .forEach(this::nodeChanged);
    }

    private void processEvent(WatchedEvent event)
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        switch ( event.getType() )
        {
        case NodeDataChanged:
        case NodeCreated:
        {
            nodeChanged(event.getPath());
            break;
        }

        case NodeDeleted:
        {
            removeStorage(event.getPath());
            break;
        }

        case NodeChildrenChanged:
        {
            nodeChildrenChanged(event.getPath());
            break;
        }
        }
    }

    private void nodeChildrenChanged(String fromPath)
    {
        if ( (state.get() != State.STARTED) || !recursive )
        {
            return;
        }

        try
        {
            BackgroundCallback callback = (__, event) -> {
                if ( event.getResultCode() == OK.intValue() )
                {
                    event.getChildren().forEach(child -> nodeChanged(ZKPaths.makePath(fromPath, child)));
                }
                else if ( event.getResultCode() == NONODE.intValue() )
                {
                    removeStorage(event.getPath());
                }
                else
                {
                    handleException(event);
                }
                checkDecrementOutstandingOps();
            };

            checkIncrementOutstandingOps();
            client.getChildren().inBackground(callback).forPath(fromPath);
        }
        catch ( Exception e )
        {
            handleException(e);
        }
    }

    private void nodeChanged(String fromPath)
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        try
        {
            BackgroundCallback callback = (__, event) -> {
                if ( event.getResultCode() == OK.intValue() )
                {
                    putStorage(new ChildData(event.getPath(), event.getStat(), event.getData()));
                    nodeChildrenChanged(event.getPath());
                }
                else if ( event.getResultCode() == NONODE.intValue() )
                {
                    removeStorage(event.getPath());
                }
                else
                {
                    handleException(event);
                }
                checkDecrementOutstandingOps();
            };

            checkIncrementOutstandingOps();
            if ( compressedData )
            {
                client.getData().decompressed().inBackground(callback).forPath(fromPath);
            }
            else
            {
                client.getData().inBackground(callback).forPath(fromPath);
            }
        }
        catch ( Exception e )
        {
            handleException(e);
        }
    }

    public static volatile boolean JZTEST = false;

    private void putStorage(ChildData data)
    {
        Optional<ChildData> previousData = storage.put(data);
        if ( previousData.isPresent() )
        {
            if ( previousData.get().getStat().getMzxid() != data.getStat().getMzxid() )
            {
                callListeners(l -> l.event(NODE_CHANGED, previousData.get(), data));
            }
        }
        else
        {
            callListeners(l -> l.event(NODE_CREATED, null, data));
        }
    }

    private void removeStorage(String path)
    {
        storage.remove(path).ifPresent(previousData -> callListeners(l -> l.event(NODE_DELETED, previousData, null)));
    }

    private void callListeners(Consumer<CuratorCacheListener> proc)
    {
        if ( state.get() == State.STARTED )
        {
            executor.execute(() -> listenerManager.forEach(proc));
        }
    }

    private void handleException(CuratorEvent event)
    {
        handleException(KeeperException.create(KeeperException.Code.get(event.getResultCode())));
    }

    private void handleException(Exception e)
    {
        ThreadUtils.checkInterrupted(e);
        exceptionHandler.accept(e);
    }

    private void checkIncrementOutstandingOps()
    {
        AtomicLong localOutstandingOps = outstandingOps;
        if ( localOutstandingOps != null )
        {
            localOutstandingOps.incrementAndGet();
        }
    }

    private void checkDecrementOutstandingOps()
    {
        AtomicLong localOutstandingOps = outstandingOps;
        if ( localOutstandingOps != null )
        {
            if ( localOutstandingOps.decrementAndGet() == 0 )
            {
                outstandingOps = null;
                callListeners(CuratorCacheListener::initialized);
            }
        }
    }
}
