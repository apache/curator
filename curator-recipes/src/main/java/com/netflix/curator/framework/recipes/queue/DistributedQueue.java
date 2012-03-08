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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
public class DistributedQueue<T> implements QueueBase<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
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
    private final AtomicReference<ErrorMode> errorMode = new AtomicReference<ErrorMode>(ErrorMode.REQUEUE);
    private final ListenerContainer<QueuePutListener<T>> putListenerContainer = new ListenerContainer<QueuePutListener<T>>();
    private final AtomicInteger lastChildCount = new AtomicInteger(0);

    private final AtomicInteger     putCount = new AtomicInteger(0);
    private final CuratorListener   listener = new CuratorListener()
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
    };

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    private enum ProcessType
    {
        NORMAL,
        REMOVE
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
    @Override
    public void     start() throws Exception
    {
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            throw new IllegalStateException();
        }

        try
        {
            client.create().creatingParentsIfNeeded().forPath(queuePath);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // this is OK
        }
        if ( lockPath != null )
        {
            try
            {
                client.create().creatingParentsIfNeeded().forPath(lockPath);
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // this is OK
            }
        }

        client.getCuratorListenable().addListener(listener, executor);

        if ( !isProducerOnly )
        {
            service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call()
                    {
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

        putListenerContainer.clear();
        client.getCuratorListenable().removeListener(listener);
        service.shutdownNow();
    }

    /**
     * Return the manager for put listeners
     *
     * @return put listener container
     */
    @Override
    public ListenerContainer<QueuePutListener<T>> getPutListenerContainer()
    {
        return putListenerContainer;
    }

    /**
     * Used when the queue is created with a {@link QueueBuilder#lockPath(String)}. Determines
     * the behavior when the queue consumer throws an exception
     *
     * @param newErrorMode the new error mode (the default is {@link ErrorMode#REQUEUE}
     */
    @Override
    public void     setErrorMode(ErrorMode newErrorMode)
    {
        Preconditions.checkNotNull(lockPath);

        errorMode.set(newErrorMode);
    }

    /**
     * Wait until any pending puts are committed
     *
     * @param waitTime max wait time
     * @param timeUnit time unit
     * @return true if the flush was successful, false if it timed out first
     * @throws InterruptedException if thread was interrupted
     */
    @Override
    public boolean flushPuts(long waitTime, TimeUnit timeUnit) throws InterruptedException
    {
        long    msWaitRemaining = TimeUnit.MILLISECONDS.convert(waitTime, timeUnit);
        synchronized(putCount)
        {
            while ( putCount.get() > 0 )
            {
                if ( msWaitRemaining <= 0 )
                {
                    return false;
                }

                long        startMs = System.currentTimeMillis();

                putCount.wait(msWaitRemaining);

                long        elapsedMs = System.currentTimeMillis() - startMs;
                msWaitRemaining -= elapsedMs;
            }
        }
        return true;
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

    /**
     * Return the most recent message count from the queue. This is useful for debugging/information
     * purposes only.
     *
     * @return count (can be 0)
     */
    @Override
    public int getLastMessageCount()
    {
        return lastChildCount.get();
    }

    void internalPut(final T item, MultiItem<T> multiItem, String path) throws Exception
    {
        final MultiItem<T> givenMultiItem = multiItem;
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

        putCount.incrementAndGet();
        byte[]              bytes = ItemSerializer.serialize(multiItem, serializer);
        BackgroundCallback  callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getType() == CuratorEventType.CREATE )
                {
                    synchronized(putCount)
                    {
                        putCount.decrementAndGet();
                        putCount.notifyAll();
                    }
                }

                putListenerContainer.forEach
                (
                    new Function<QueuePutListener<T>, Void>()
                    {
                        @Override
                        public Void apply(QueuePutListener<T> listener)
                        {
                            if ( item != null )
                            {
                                listener.putCompleted(item);
                            }
                            else
                            {
                                listener.putMultiCompleted(givenMultiItem);
                            }
                            return null;
                        }
                    }
                );
            }
        };
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground(callback).forPath(path, bytes);
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

    protected void sortChildren(List<String> children)
    {
        Collections.sort(children);
    }

    protected List<String> getChildren() throws Exception
    {
        return client.getChildren().watched().forPath(queuePath);
    }
    
    protected boolean tryRemove(String itemNode) throws Exception
    {
        boolean     isUsingLockSafety = (lockPath != null);
        if ( isUsingLockSafety )
        {
            return processWithLockSafety(itemNode, ProcessType.REMOVE);
        }

        return processNormally(itemNode, ProcessType.REMOVE);
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
                        children = getChildren();
                        lastChildCount.set(children.size());
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
            log.error("Exception caught in background handler", e);
        }
    }

    private void processChildren(List<String> children) throws Exception
    {
        sortChildren(children); // makes sure items are processed in the order they were added

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
                log.warn("Foreign node in queue path: " + itemNode);
                continue;
            }

            if ( min-- <= 0 )
            {
                if ( refreshOnWatchSignaled.compareAndSet(true, false) )
                {
                    break;
                }
            }

            if ( isUsingLockSafety )
            {
                processWithLockSafety(itemNode, ProcessType.NORMAL);
            }
            else
            {
                processNormally(itemNode, ProcessType.NORMAL);
            }
        }
    }

    private boolean processMessageBytes(String itemNode, byte[] bytes) throws Exception
    {
        MultiItem<T>    items;
        try
        {
            items = ItemSerializer.deserialize(bytes, serializer);
        }
        catch ( Exception e )
        {
            log.error("Corrupted queue item: " + itemNode, e);
            return false;
        }

        boolean     removeItem = true;
        for(;;)
        {
            T       item = items.nextItem();
            if ( item == null )
            {
                break;
            }

            try
            {
                consumer.consumeMessage(item);
            }
            catch ( Exception e )
            {
                log.error("Exception processing queue item: " + itemNode, e);
                if ( errorMode.get() == ErrorMode.REQUEUE )
                {
                    removeItem = false;
                    break;
                }
            }
        }
        return removeItem;
    }

    private boolean processNormally(String itemNode, ProcessType type) throws Exception
    {
        try
        {
            String  itemPath = ZKPaths.makePath(queuePath, itemNode);
            Stat    stat = new Stat();

            byte[]  bytes = null;
            if ( type == ProcessType.NORMAL )
            {
                bytes = client.getData().storingStatIn(stat).forPath(itemPath);
            }
            client.delete().withVersion(stat.getVersion()).forPath(itemPath);

            if ( type == ProcessType.NORMAL )
            {
                processMessageBytes(itemNode, bytes);
            }

            return true;
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

        return false;
    }

    private boolean processWithLockSafety(String itemNode, ProcessType type) throws Exception
    {
        String      lockNodePath = ZKPaths.makePath(lockPath, itemNode);
        boolean     lockCreated = false;
        try
        {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath);
            lockCreated = true;

            String  itemPath = ZKPaths.makePath(queuePath, itemNode);
            boolean doDelete = false;
            if ( type == ProcessType.NORMAL )
            {
                byte[]  bytes = client.getData().forPath(itemPath);
                doDelete = processMessageBytes(itemNode, bytes);
            }
            else if ( type == ProcessType.REMOVE )
            {
                doDelete = true;
            }

            if ( doDelete )
            {
                client.delete().forPath(itemPath);
            }

            return true;
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
                client.delete().guaranteed().forPath(lockNodePath);
            }
        }

        return false;
    }
}
