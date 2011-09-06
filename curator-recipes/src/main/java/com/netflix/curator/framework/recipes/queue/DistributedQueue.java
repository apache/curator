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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
 * <li>There is guaranteed processing of each message to the point of receipt by a given instance.
 * If an instance receives an item from the queue but dies while processing it, the item will be lost</li>
 * <li><b>IMPORTANT: </b>If you use a number greater than 1 for <code>maxInternalQueue</code> when
 * constructing a <code>DistributedQueue</code>, you risk that many items getting lost if your process
 * dies without processing them. i.e. <code>maxInternalQueue</code> items get taken out of the distributed
 * queue and placed in your local instance's in-memory queue.</li>
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
    private final AtomicReference<Exception> backgroundException = new AtomicReference<Exception>(null);
    private final int minItemsBeforeRefresh;
    private final boolean refreshOnWatch;
    private final boolean isProducerOnly;
    private final QueueSafety<T> safety;
    private final AtomicBoolean refreshOnWatchSignaled = new AtomicBoolean(false);
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
            QueueSerializer<T> serializer,
            String queuePath,
            ThreadFactory threadFactory,
            Executor executor,
            int maxInternalQueue,
            int minItemsBeforeRefresh,
            boolean refreshOnWatch,
            QueueSafety<T> safety
        )
    {
        this.minItemsBeforeRefresh = minItemsBeforeRefresh;
        this.refreshOnWatch = refreshOnWatch;
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(queuePath);
        Preconditions.checkNotNull(threadFactory);
        Preconditions.checkNotNull(executor);

        isProducerOnly = (maxInternalQueue < 0);
        if ( safety == null )
        {
            safety = makeDefaultSafety(maxInternalQueue);
        }

        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;
        this.executor = executor;
        this.safety = safety;
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
        if ( safety.getLockPath() != null )
        {
            try
            {
                client.create().creatingParentsIfNeeded().forPath(safety.getLockPath(), new byte[0]);
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

    /**
     * Take the next item off of the queue blocking until there is an item available
     *
     * @return the item
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take() throws Exception
    {
        checkState();
        Preconditions.checkNotNull(safety.getQueue());

        return safety.getQueue().take();
    }

    /**
     * Take the next item off of the queue blocking until there is an item available
     * or the specified timeout has elapsed
     *
     * @param timeout timeout
     * @param unit unit
     * @return the item or null if timed out
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take(long timeout, TimeUnit unit) throws Exception
    {
        checkState();
        Preconditions.checkNotNull(safety.getQueue());

        return safety.getQueue().poll(timeout, unit);
    }

    /**
     * Return the number of pending items in the local Java queue. IMPORTANT: when this method
     * returns a non-zero value, there is no guarantee that a subsequent call to take() will not
     * block. i.e. items can get removed between this method call and others.
     *
     * @return item qty or 0
     * @throws Exception an error in the background thread
     */
    public int      available() throws Exception
    {
        checkState();
        Preconditions.checkNotNull(safety.getQueue());

        return safety.getQueue().size();
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

        Exception exception = backgroundException.getAndSet(null);
        if ( exception != null )
        {
            throw exception;
        }
    }

    String makeItemPath()
    {
        return ZKPaths.makePath(queuePath, QUEUE_ITEM_NAME);
    }

    private QueueSafety<T> makeDefaultSafety(int maxInternalQueue)
    {
        BlockingQueue<T> internalQueue;
        if ( maxInternalQueue < 0 )
        {
            internalQueue = null;
        }
        else if ( maxInternalQueue == 0 )
        {
            internalQueue = new SynchronousQueue<T>();
        }
        else
        {
            internalQueue = new LinkedBlockingQueue<T>(maxInternalQueue);
        }
        return new QueueSafety<T>(null, internalQueue);
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
            backgroundException.set(e);
        }
    }

    private void processChildren(List<String> children) throws Exception
    {
        Collections.sort(children); // makes sure items are processed in the order they were added

        boolean     isUsingLockSafety = (safety.getLockPath() != null);
        int         min = minItemsBeforeRefresh;
        for ( String itemNode : children )
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                break;
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
                    String  lockPath = ZKPaths.makePath(safety.getLockPath(), itemNode);
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(lockPath, new byte[0]);
                    lockCreated = true;
                }
                else
                {
                    client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                }

                MultiItem<T>    items = ItemSerializer.deserialize(bytes, serializer);
                for(;;)
                {
                    T       item = items.nextItem();
                    if ( item == null )
                    {
                        break;
                    }
                    
                    if ( safety.getConsumer() != null )
                    {
                        safety.getConsumer().consumeMessage(item);
                    }
                    else
                    {
                        safety.getQueue().put(item);
                    }
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
                    String  lockPath = ZKPaths.makePath(safety.getLockPath(), itemNode);
                    client.delete().forPath(lockPath);
                }
            }
        }
    }
}
