package com.netflix.curator.framework.recipes.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final QueueSharderPolicies      policies;
    private final ConcurrentMap<String, T>  queues = Maps.newConcurrentMap();
    private final Set<String>               preferredQueues = Sets.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());
    private final AtomicReference<State>    state = new AtomicReference<State>(State.LATENT);
    private final LeaderSelector            leaderSelector;
    private final AtomicBoolean             isLeader = new AtomicBoolean(false);
    private final Random                    random = new Random();
    private final ExecutorService           service;

    private final CuratorWatcher            watcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            updateQueues();
        }
    };

    @SuppressWarnings("FieldCanBeLocal")
    private final LeaderSelectorListener    listener = new LeaderSelectorListener()
    {
        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
            try
            {
                isLeader.set(true);
                Thread.currentThread().join();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            finally
            {
                isLeader.set(false);
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
        }
    };

    private static final String         QUEUE_PREFIX = "queue-";

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client client
     * @param queueAllocator allocator for new queues
     * @param queuePath path for the queues
     * @param leaderPath path for the leader that monitors queue sizes (must be different than queuePath)
     * @param policies sharding policies
     */
    public QueueSharder(CuratorFramework client, QueueAllocator<U, T> queueAllocator, String queuePath, String leaderPath, QueueSharderPolicies policies)
    {
        this.client = client;
        this.queueAllocator = queueAllocator;
        this.queuePath = queuePath;
        this.policies = policies;
        leaderSelector = new LeaderSelector(client, leaderPath, listener);
        service = Executors.newSingleThreadExecutor(policies.getThreadFactory());
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

        updateQueues();
        leaderSelector.start();

        service.submit
        (
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    try
                    {
                        while ( !Thread.currentThread().isInterrupted() )
                        {
                            Thread.sleep(policies.getThresholdCheckMs());
                            checkThreshold();
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
            }
        );
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            service.shutdownNow();
            Closeables.closeQuietly(leaderSelector);

            for ( T queue : queues.values() )
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

        List<String>    localPreferredQueues = Lists.newArrayList(preferredQueues);
        if ( localPreferredQueues.size() > 0 )
        {
            String      key = localPreferredQueues.get(random.nextInt(localPreferredQueues.size()));
            return queues.get(key);
        }

        List<String>    keys = Lists.newArrayList(queues.keySet());
        String          key = keys.get(random.nextInt(keys.size()));
        return queues.get(key);
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

    @VisibleForTesting
    Collection<String>  getQueuePaths()
    {
        return queues.keySet();
    }

    private void updateQueues() throws Exception
    {
        List<String>        children = client.getChildren().usingWatcher(watcher).forPath(queuePath);
        for ( String child : children )
        {
            String              queuePath = ZKPaths.makePath(this.queuePath, child);
            addNewQueueIfNeeded(queuePath);
        }

        if ( children.size() == 0 )
        {
            addNewQueueIfNeeded(null);
        }
    }

    private void addNewQueueIfNeeded(String newQueuePath) throws Exception
    {
        if ( newQueuePath == null )
        {
            newQueuePath = ZKPaths.makePath(queuePath, QUEUE_PREFIX + UUID.randomUUID().toString());
        }
        T                   queue = queueAllocator.allocateQueue(client, newQueuePath);
        if ( queues.putIfAbsent(newQueuePath, queue) == null )
        {
            queue.start();
            preferredQueues.add(newQueuePath);
        }
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
                if ( stat.getNumChildren() >= policies.getNewQueueThreshold() )
                {
                    if ( preferredQueues.contains(queuePath) )  // otherwise a queue has already been added for this
                    {
                        size = stat.getNumChildren();
                        addAQueue = true;
                        preferredQueues.remove(queuePath);
                    }
                }
                else if ( stat.getNumChildren() <= (policies.getNewQueueThreshold() / 2) )
                {
                    preferredQueues.add(queuePath);
                }
            }

            if ( addAQueue && isLeader.get() )
            {
                if ( queues.size() < policies.getMaxQueues() )
                {
                    log.info(String.format("Adding a queue due to exceeded threshold. Queue Size: %d - Threshold: %d", size, policies.getNewQueueThreshold()));

                    addNewQueueIfNeeded(null);
                }
                else
                {
                    log.warn(String.format("Max number of queues (%d) reached. Consider increasing the max.", policies.getMaxQueues()));
                }
            }
        }
        catch ( Exception e )
        {
            log.error("Checking queue counts against threshold", e);
        }
    }
}
