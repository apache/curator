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
package org.apache.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
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
    private final LeaderLatch               leaderLatch;
    private final Random                    random = new Random();
    private final ExecutorService           service;

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
        leaderLatch = new LeaderLatch(client, leaderPath);
        service = Executors.newSingleThreadExecutor(policies.getThreadFactory());
    }

    /**
     * The sharder must be started
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        client.createContainers(queuePath);

        getInitialQueues();
        leaderLatch.start();

        service.submit
        (
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    while ( state.get() == State.STARTED )
                    {
                        try
                        {
                            Thread.sleep(policies.getThresholdCheckMs());
                            checkThreshold();
                        }
                        catch ( InterruptedException e )
                        {
                            // swallow the interrupt as it's only possible from either a background
                            // operation and, thus, doesn't apply to this loop or the instance
                            // is being closed in which case the while test will get it
                        }
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
            CloseableUtils.closeQuietly(leaderLatch);

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

    /**
     * Return the current set of shard paths
     *
     * @return paths
     */
    public Collection<String>  getQueuePaths()
    {
        return ImmutableSet.copyOf(queues.keySet());
    }

    private void getInitialQueues() throws Exception
    {
        List<String>        children = client.getChildren().forPath(queuePath);
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

        if ( !queues.containsKey(newQueuePath) )
        {
            T                   queue = queueAllocator.allocateQueue(client, newQueuePath);
            if ( queues.putIfAbsent(newQueuePath, queue) == null )
            {
                queue.start();
                preferredQueues.add(newQueuePath);
            }
        }
    }

    private void checkThreshold()
    {
        try
        {
            boolean             addAQueueIfLeader = false;
            int                 size = 0;
            List<String>        children = client.getChildren().forPath(queuePath);
            for ( String child : children )
            {
                String  queuePath = ZKPaths.makePath(this.queuePath, child);
                addNewQueueIfNeeded(queuePath);

                Stat    stat = client.checkExists().forPath(queuePath);
                if ( stat.getNumChildren() >= policies.getNewQueueThreshold() )
                {
                    size = stat.getNumChildren();
                    addAQueueIfLeader = true;
                    preferredQueues.remove(queuePath);
                }
                else if ( stat.getNumChildren() <= (policies.getNewQueueThreshold() / 2) )
                {
                    preferredQueues.add(queuePath);
                }
            }

            if ( addAQueueIfLeader && leaderLatch.hasLeadership() )
            {
                if ( queues.size() < policies.getMaxQueues() )
                {
                    log.info("Adding a queue due to exceeded threshold. Queue Size: {} - Threshold: {}", size, policies.getNewQueueThreshold());

                    addNewQueueIfNeeded(null);
                }
                else
                {
                    log.warn("Max number of queues ({}) reached. Consider increasing the max.", policies.getMaxQueues());
                }
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            log.error("Checking queue counts against threshold", e);
        }
    }
}
