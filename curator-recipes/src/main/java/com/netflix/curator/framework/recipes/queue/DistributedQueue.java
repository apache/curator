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
package com.netflix.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>An implementation of the Distributed Queue ZK recipe. Items put into the queue
 * are guaranteed to be ordered (by means of ZK's PERSISTENT_SEQUENTIAL node).</p>
 *
 * <p>
 * Guarantees:<br/>
 * <li>If a single consumer takes items out of the queue, they will be ordered FIFO. i.e. if ordering is important,
 * use a {@link LeaderSelector} to nominate a single consumer.</li>
 * <li>Unless a {@link QueueBuilder#lockPath(String)} is used, there is only guaranteed processing of each message to the point of receipt by a given instance.
 * If an instance receives an item from the queue but dies while processing it, the item will be lost. If you need message recoverability, use
 * a {@link QueueBuilder#lockPath(String)}</li>
 * </p>
 */
public class DistributedQueue<T> implements Closeable
{
    private final CuratorFramework client;
    private final QueueSerializer<T> serializer;
    private final String queuePath;
    private final Executor executor;
    private final ExecutorService service;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final QueueConsumer<T> consumer;
    private final int minItemsBeforeRefresh;
    private final boolean refreshOnWatch;
    private final boolean isProducerOnly;
    private final AtomicBoolean refreshOnWatchSignaled = new AtomicBoolean(false);
    private final String lockPath;
    private final CuratorListener listener = new CuratorListener()
    {
        @Override
        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getType() == CuratorEventType.WATCHED )
            {
                if ( event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeChildrenChanged )
                {
                    internalNotify();
                }
            }
        }

        @Override
        public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
        {
            // nop
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    private static final String     QUEUE_ITEM_NAME = "queue-";

    DistributedQueue
        (
            CuratorFramework client,
            QueueConsumer<T> consumer,
            QueueSerializer<T> serializer,
            String queuePath,
            ThreadFactory threadFactory,
            Executor executor,
            int minItemsBeforeRefresh,
            boolean refreshOnWatch,
            String lockPath
        )
    {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(queuePath);
        Preconditions.checkNotNull(threadFactory);
        Preconditions.checkNotNull(executor);

        isProducerOnly = (consumer == null);
        this.lockPath = lockPath;
        this.consumer = consumer;
        this.minItemsBeforeRefresh = minItemsBeforeRefresh;
        this.refreshOnWatch = refreshOnWatch;
        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;
        this.executor = executor;
        service = Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
    public void     start() throws Exception
    {
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            throw new IllegalStateException();
        }

        try
        {
            client.create().creatingParentsIfNeeded().forPath(queuePath, new byte[0]);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // this is OK
        }
        if ( lockPath != null )
        {
            try
            {
                client.create().creatingParentsIfNeeded().forPath(lockPath, new byte[0]);
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // this is OK
            }
        }

        client.addListener(listener, executor);

        if ( !isProducerOnly )
        {
            service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call()
                    {
                        Thread.currentThread().setName("Curator-DistributedQueue");

                        runLoop();
                        return null;
                    }
                }
            );
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( !state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            throw new IllegalStateException();
        }

        client.removeListener(listener);
        service.shutdownNow();
    }

    /**
     * Add an item into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param item item to add
     * @throws Exception connection issues
     */
    public void     put(T item) throws Exception
    {
        checkState();

        String      path = makeItemPath();
        internalPut(item, null, path);
    }

    /**
     * Add a set of items into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param items items to add
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items) throws Exception
    {
        checkState();

        String      path = makeItemPath();
        internalPut(null, items, path);
    }

    void internalPut(T item, MultiItem<T> multiItem, String path) throws Exception
    {
        if ( item != null )
        {
            final AtomicReference<T>    ref = new AtomicReference<T>(item);
            multiItem = new MultiItem<T>()
            {
                @Override
                public T nextItem() throws Exception
                {
                    return ref.getAndSet(null);
                }
            };
        }

        byte[]      bytes = ItemSerializer.serialize(multiItem, serializer);
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground().forPath(path, bytes);
    }

    void checkState() throws Exception
    {
        if ( state.get() != State.STARTED )
        {
            throw new IllegalStateException();
        }
    }

    String makeItemPath()
    {
        return ZKPaths.makePath(queuePath, QUEUE_ITEM_NAME);
    }

    private synchronized void internalNotify()
    {
        if ( refreshOnWatch )
        {
            refreshOnWatchSignaled.set(true);
        }
        notifyAll();
    }

    private void runLoop()
    {
        try
        {
            while ( !Thread.currentThread().isInterrupted()  )
            {
                List<String>        children;
                synchronized(this)
                {
                    do
                    {
                        children = client.getChildren().watched().forPath(queuePath);
                        if ( children.size() == 0 )
                        {
                            wait();
                        }
                    } while ( children.size() == 0 );

                    refreshOnWatchSignaled.set(false);
                }
                if ( children.size() > 0 )
                {
                    processChildren(children);
                }
            }
        }
        catch ( Exception e )
        {
            client.getZookeeperClient().getLog().error("Exception caught in background handler", e);
        }
    }

    private void processChildren(List<String> children) throws Exception
    {
        Collections.sort(children); // makes sure items are processed in the order they were added

        boolean     isUsingLockSafety = (lockPath != null);
        int         min = minItemsBeforeRefresh;
        for ( String itemNode : children )
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                break;
            }

            if ( !itemNode.startsWith(QUEUE_ITEM_NAME) )
            {
                client.getZookeeperClient().getLog().warn("Foreign node in queue path: " + itemNode);
                continue;
            }

            if ( min-- <= 0 )
            {
                if ( refreshOnWatchSignaled.compareAndSet(true, false) )
                {
                    break;
                }
            }

            boolean     lockCreated = false;
            String      itemPath = ZKPaths.makePath(queuePath, itemNode);
            try
            {
                Stat    stat = new Stat();
                byte[]  bytes = client.getData().storingStatIn(stat).forPath(itemPath);
                if ( isUsingLockSafety )
                {
                    String  lockNodePath = ZKPaths.makePath(lockPath, itemNode);
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath, new byte[0]);
                    lockCreated = true;
                }
                else
                {
                    client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                }

                MultiItem<T>    items;
                try
                {
                    items = ItemSerializer.deserialize(bytes, serializer);
                }
                catch ( Exception e )
                {
                    client.getZookeeperClient().getLog().error("Corrupted queue item: " + itemNode);
                    continue;
                }

                for(;;)
                {
                    T       item = items.nextItem();
                    if ( item == null )
                    {
                        break;
                    }

                    consumer.consumeMessage(item);
                }

                if ( isUsingLockSafety )
                {
                    client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                }
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // another process got it
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // another process got it
            }
            catch ( KeeperException.BadVersionException ignore )
            {
                // another process got it
            }
            finally
            {
                if ( lockCreated )
                {
                    String  lockNodePath = ZKPaths.makePath(lockPath, itemNode);
                    client.delete().forPath(lockNodePath);
                }
            }
        }
    }
}
