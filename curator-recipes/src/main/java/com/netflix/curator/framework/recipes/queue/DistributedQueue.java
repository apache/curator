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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
import java.util.concurrent.Semaphore;
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
    private final int maxItems;
    private final int finalFlushMs;
    private final boolean putInBackground;
    private final ChildrenCache childrenCache;

    private final AtomicInteger     putCount = new AtomicInteger(0);

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    @VisibleForTesting
    protected enum ProcessType
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
            String lockPath,
            int maxItems,
            boolean putInBackground,
            int finalFlushMs
        )
    {
        Preconditions.checkNotNull(client, "client cannot be null");
        Preconditions.checkNotNull(serializer, "serializer cannot be null");
        Preconditions.checkNotNull(queuePath, "queuePath cannot be null");
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
        Preconditions.checkNotNull(executor, "executor cannot be null");
        Preconditions.checkArgument(maxItems > 0, "maxItems must be a positive number");

        isProducerOnly = (consumer == null);
        this.lockPath = lockPath;
        this.putInBackground = putInBackground;
        this.consumer = consumer;
        this.minItemsBeforeRefresh = minItemsBeforeRefresh;
        this.refreshOnWatch = refreshOnWatch;
        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;
        this.executor = executor;
        this.maxItems = maxItems;
        this.finalFlushMs = finalFlushMs;
        service = Executors.newSingleThreadExecutor(threadFactory);
        childrenCache = new ChildrenCache(client, queuePath)
        {
            @Override
            protected synchronized void notifyFromCallback()
            {
                if ( DistributedQueue.this.refreshOnWatch )
                {
                    refreshOnWatchSignaled.set(true);
                }
                super.notifyFromCallback();
            }
        };

        if ( (maxItems != QueueBuilder.NOT_SET) && putInBackground )
        {
            log.warn("Bounded queues should set putInBackground(false) in the builder. Putting in the background will result in spotty maxItem consistency.");
        }
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

        if ( !isProducerOnly || (maxItems != QueueBuilder.NOT_SET) )
        {
            childrenCache.start();
        }

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
        if ( state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            if ( finalFlushMs > 0 )
            {
                try
                {
                    flushPuts(finalFlushMs, TimeUnit.MILLISECONDS);
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            }

            Closeables.closeQuietly(childrenCache);
            putListenerContainer.clear();
            service.shutdownNow();
        }
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
        Preconditions.checkNotNull(lockPath, "lockPath cannot be null");

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
     * return quickly.<br/><br/>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param item item to add
     * @throws Exception connection issues
     */
    public void     put(T item) throws Exception
    {
        put(item, 0, null);
    }

    /**
     * Same as {@link #put(Object)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param item item to add
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean     put(T item, int maxWait, TimeUnit unit) throws Exception
    {
        checkState();

        String      path = makeItemPath();
        return internalPut(item, null, path, maxWait, unit);
    }

    /**
     * Add a set of items into the queue. Adding is done in the background - thus, this method will
     * return quickly.<br/><br/>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param items items to add
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items) throws Exception
    {
        putMulti(items, 0, null);
    }

    /**
     * Same as {@link #putMulti(MultiItem)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param items items to add
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean     putMulti(MultiItem<T> items, int maxWait, TimeUnit unit) throws Exception
    {
        checkState();

        String      path = makeItemPath();
        return internalPut(null, items, path, maxWait, unit);
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

    boolean internalPut(final T item, MultiItem<T> multiItem, String path, int maxWait, TimeUnit unit) throws Exception
    {
        if ( !blockIfMaxed(maxWait, unit) )
        {
            return false;
        }

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
        if ( putInBackground )
        {
            doPutInBackground(item, path, givenMultiItem, bytes);
        }
        else
        {
            doPutInForeground(item, path, givenMultiItem, bytes);
        }
        return true;
    }

    private void doPutInForeground(final T item, String path, final MultiItem<T> givenMultiItem, byte[] bytes) throws Exception
    {
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, bytes);
        synchronized(putCount)
        {
            putCount.decrementAndGet();
            putCount.notifyAll();
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

    private void doPutInBackground(final T item, String path, final MultiItem<T> givenMultiItem, byte[] bytes) throws Exception
    {
        BackgroundCallback callback = new BackgroundCallback()
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
        internalCreateNode(path, bytes, callback);
    }

    @VisibleForTesting
    void internalCreateNode(String path, byte[] bytes, BackgroundCallback callback) throws Exception
    {
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
        return client.getChildren().forPath(queuePath);
    }

    protected long getDelay(String itemNode)
    {
        return 0;
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

    private boolean blockIfMaxed(int maxWait, TimeUnit unit) throws Exception
    {
        ChildrenCache.Data data = childrenCache.getData();
        while ( data.children.size() >= maxItems )
        {
            long        previousVersion = data.version;
            data = childrenCache.blockingNextGetData(data.version, maxWait, unit);
            if ( data.version == previousVersion )
            {
                return false;
            }
        }
        return true;
    }

    private void runLoop()
    {
        long         currentVersion = -1;
        long         maxWaitMs = -1;
        try
        {
            while ( !Thread.currentThread().isInterrupted()  )
            {
                ChildrenCache.Data      data = (maxWaitMs > 0) ? childrenCache.blockingNextGetData(currentVersion, maxWaitMs, TimeUnit.MILLISECONDS) : childrenCache.blockingNextGetData(currentVersion);
                currentVersion = data.version;

                List<String>        children = Lists.newArrayList(data.children);
                sortChildren(children); // makes sure items are processed in the correct order

                if ( children.size() > 0 )
                {
                    maxWaitMs = getDelay(children.get(0));
                    if ( maxWaitMs > 0 )
                    {
                        continue;
                    }
                }
                else
                {
                    continue;
                }

                refreshOnWatchSignaled.set(false);
                processChildrenAndReset(children);
            }
        }
        catch ( InterruptedException ignore )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Exception e )
        {
            log.error("Exception caught in background handler", e);
        }
    }

    private void processChildrenAndReset(List<String> children) throws Exception
    {
        try
        {
            processChildren(children);
        }
        finally
        {
            childrenCache.sync();
        }
    }

    private void processChildren(List<String> children) throws Exception
    {
        final Semaphore processedLatch = new Semaphore(0);
        final boolean   isUsingLockSafety = (lockPath != null);
        int             min = minItemsBeforeRefresh;
        for ( final String itemNode : children )
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                processedLatch.release(children.size());
                break;
            }

            if ( !itemNode.startsWith(QUEUE_ITEM_NAME) )
            {
                log.warn("Foreign node in queue path: " + itemNode);
                processedLatch.release();
                continue;
            }

            if ( min-- <= 0 )
            {
                if ( refreshOnWatchSignaled.compareAndSet(true, false) )
                {
                    processedLatch.release(children.size());
                    break;
                }
            }

            if ( getDelay(itemNode) > 0 )
            {
                processedLatch.release();
                continue;
            }

            executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            if ( isUsingLockSafety )
                            {
                                processWithLockSafety(itemNode, ProcessType.NORMAL);
                            }
                            else
                            {
                                processNormally(itemNode, ProcessType.NORMAL);
                            }
                        }
                        catch ( Exception e )
                        {
                            log.error("Error processing message at " + itemNode, e);
                        }
                        finally
                        {
                            processedLatch.release();
                        }
                    }
                }
            );
        }

        processedLatch.acquire(children.size());
    }

    private boolean processMessageBytes(String itemNode, byte[] bytes) throws Exception
    {
        MultiItem<T>    items;
        try
        {
            items = ItemSerializer.deserialize(bytes, serializer);
        }
        catch ( Throwable e )
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
            catch ( Throwable e )
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
            if ( client.isStarted() )
            {
                client.delete().withVersion(stat.getVersion()).forPath(itemPath);
            }

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

    @VisibleForTesting
    protected boolean processWithLockSafety(String itemNode, ProcessType type) throws Exception
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
