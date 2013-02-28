package com.netflix.curator.x.sync;

import com.google.common.collect.ImmutableList;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

class NodeSyncQueue implements Closeable
{
    private final CuratorGlobalSync sync;
    private final ExecutorService service;
    private final Collection<CuratorFramework> remoteClients;
    private final BlockingQueue<ChildDataHolder> queue = new LinkedBlockingQueue<ChildDataHolder>();    // TODO capacity

    private static class ChildDataHolder
    {
        private final ChildData data;

        private ChildDataHolder(ChildData data)
        {
            this.data = data;
        }

        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            ChildDataHolder that = (ChildDataHolder)o;
            return data.getPath().equals(that.data.getPath());
        }

        @Override
        public int hashCode()
        {
            return data.getPath().hashCode();
        }
    }

    NodeSyncQueue(CuratorGlobalSync sync, ExecutorService service, Collection<CuratorFramework> remoteClients)
    {
        this.sync = sync;
        this.service = service;
        this.remoteClients = ImmutableList.copyOf(remoteClients);
    }

    void start()
    {
        service.submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        while ( !Thread.currentThread().isInterrupted() )
                        {
                            process(queue.take());
                        }
                    }
                    catch ( InterruptedException dummy )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        );
    }

    private void process(ChildDataHolder childDataHolder)
    {
        for ( final CuratorFramework remote : remoteClients )
        {
            service.submit(new RemoteSync(sync, remote, childDataHolder.data));
        }
    }

    @Override
    public void close()
    {
        service.shutdownNow();
    }

    synchronized void add(ChildData nodeData)
    {
        // TODO what about deletes?
        queue.add(new ChildDataHolder(nodeData));
    }
}
