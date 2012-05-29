package com.netflix.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     A utility for shard a distributed queue.
 * </p>
 *
 * <p>
 *     Due to limitations in ZooKeeper's transport layer,
 *     a single queue will break if it has more than 10K-ish items in it. This class
 *     provides a facade over multiple distributed queues. It monitors the queues and if
 *     any one of them goes over a threshold, a new queue is added. Puts are distributed
 *     amongst the queues.
 * </p>
 *
 * <p>
 *     NOTE: item ordering is maintained within each managed queue but cannot be maintained across
 *     queues. i.e. items might get consumed out of order if they are in different managed
 *     queues.
 * </p>
 */
public class QueueSharder<U, T extends QueueBase<U>> implements Closeable
{
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final QueueAllocator<U, T>      queueAllocator;
    private final String                    queuePath;
    private final int                       newQueueThreshold;
    private final int                       thresholdCheckMs;
    private final List<T>                   queues = new CopyOnWriteArrayList<T>();
    private final AtomicInteger             nextIndex = new AtomicInteger();
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private final LeaderSelector            leaderSelector;
    @SuppressWarnings("FieldCanBeLocal")
    private final LeaderSelectorListener    listener = new LeaderSelectorListener()
    {
        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
            try
            {
                while ( !Thread.currentThread().isInterrupted() )
                {
                    Thread.sleep(thresholdCheckMs);
                    checkThreshold();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
        }
    };

    private static final String         QUEUE_PREFIX = "queue-";

    private static final int            DEFAULT_THRESHOLD_CHECK_MS = 60000;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client the client
     * @param queueAllocator used to allocate new queues
     * @param queuePath the path to use to hold the queues
     * @param leaderPath a path for a leader selector - used to monitor the threshold
     * @param newQueueThreshold the threshold at which to add new queues. i.e. if any queue gets
     *                          to this number of items, a new queue will be added
     */
    public QueueSharder(CuratorFramework client, QueueAllocator<U, T> queueAllocator, String queuePath, String leaderPath, int newQueueThreshold)
    {
        this(client, queueAllocator, queuePath, leaderPath, newQueueThreshold, DEFAULT_THRESHOLD_CHECK_MS);
    }

    /**
     * @param client the client
     * @param queueAllocator used to allocate new queues
     * @param queuePath the path to use to hold the queues
     * @param leaderPath a path for a leader selector - used to monitor the threshold
     * @param newQueueThreshold the threshold at which to add new queues. i.e. if any queue gets
     *                          to this number of items, a new queue will be added
     * @param thresholdCheckMs how often to monitor for thresholds
     */
    public QueueSharder(CuratorFramework client, QueueAllocator<U, T> queueAllocator, String queuePath, String leaderPath, int newQueueThreshold, int thresholdCheckMs)
    {
        this.client = client;
        this.queueAllocator = queueAllocator;
        this.queuePath = queuePath;
        this.newQueueThreshold = newQueueThreshold;
        this.thresholdCheckMs = thresholdCheckMs;
        leaderSelector = new LeaderSelector(client, leaderPath, listener);
    }

    /**
     * The sharder must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.newNamespaceAwareEnsurePath(queuePath).ensure(client.getZookeeperClient());

        loadExistingQueues();
        leaderSelector.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            Closeables.closeQuietly(leaderSelector);

            for ( T queue : queues )
            {
                try
                {
                    queue.close();
                }
                catch ( IOException e )
                {
                    log.error("Closing a queue", e);
                }
            }
        }
    }

    /**
     * Return one of the managed queues - the selection method cannot be relied on. It should
     * be considered a random managed queue.
     *
     * @return a queue
     */
    public T    getQueue()
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        int         index = Math.abs(nextIndex.incrementAndGet());
        return queues.get(index % queues.size());
    }

    /**
     * Return the current number of mananged queues
     *
     * @return qty
     */
    public int      getShardQty()
    {
        return queues.size();
    }

    private void loadExistingQueues() throws Exception
    {
        List<String>        children = client.getChildren().forPath(queuePath);
        for ( String child : children )
        {
            String              queuePath = ZKPaths.makePath(this.queuePath, child);
            addNewQueue(queuePath);
        }

        if ( children.size() == 0 )
        {
            addNewQueue(null);
        }
    }

    private void addNewQueue(String newQueuePath) throws Exception
    {
        if ( newQueuePath == null )
        {
            newQueuePath = ZKPaths.makePath(queuePath, QUEUE_PREFIX + UUID.randomUUID().toString());
        }
        T                   queue = queueAllocator.allocateQueue(client, newQueuePath);
        queues.add(queue);
        queue.start();
    }

    private void checkThreshold()
    {
        try
        {
            boolean             addAQueue = false;
            int                 size = 0;
            List<String>        children = client.getChildren().forPath(queuePath);
            for ( String child : children )
            {
                String  queuePath = ZKPaths.makePath(this.queuePath, child);
                Stat    stat = client.checkExists().forPath(queuePath);
                if ( stat.getNumChildren() >= newQueueThreshold )
                {
                    size = stat.getNumChildren();
                    addAQueue = true;
                    break;
                }
            }

            if ( addAQueue )
            {
                log.info(String.format("Adding a queue due to exceeded threshold. Queue Size: %d - Threshold: %d", size, newQueueThreshold));

                addNewQueue(null);
            }
        }
        catch ( Exception e )
        {
            log.error("Checking queue counts against threshold", e);
        }
    }
}
