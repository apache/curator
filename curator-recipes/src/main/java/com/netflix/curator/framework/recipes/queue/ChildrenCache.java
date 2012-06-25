package com.netflix.curator.framework.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ChildrenCache implements Closeable
{
    private final CuratorFramework client;
    private final String path;
    private final AtomicReference<Data> children = new AtomicReference<Data>(new Data(Lists.<String>newArrayList(), 0));
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final CuratorWatcher watcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( !isClosed.get() )
            {
                sync();
            }
        }
    };

    private final BackgroundCallback  callback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            setNewChildren(event.getChildren());
        }
    };

    static class Data
    {
        final List<String>      children;
        final long              version;

        private Data(List<String> children, long version)
        {
            this.children = ImmutableList.copyOf(children);
            this.version = version;
        }
    }

    ChildrenCache(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = path;
    }

    void start() throws Exception
    {
        sync();
    }

    @Override
    public void close() throws IOException
    {
        isClosed.set(true);
        notifyFromCallback();
    }

    Data getData()
    {
        return children.get();
    }

    Data blockingNextGetData(long startVersion) throws InterruptedException
    {
        return blockingNextGetData(startVersion, 0, null);
    }

    synchronized Data blockingNextGetData(long startVersion, long maxWait, TimeUnit unit) throws InterruptedException
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? unit.toMillis(maxWait) : -1;
        while ( startVersion == children.get().version )
        {
            if ( hasMaxWait )
            {
                long        elapsedMs = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsedMs;
                if ( thisWaitMs <= 0 )
                {
                    break;
                }
                wait(thisWaitMs);
            }
            else
            {
                wait();
            }
        }
        return children.get();
    }

    synchronized void sync() throws Exception
    {
        client.getChildren().usingWatcher(watcher).inBackground(callback).forPath(path);
    }

    protected synchronized void notifyFromCallback()
    {
        notifyAll();
    }

    private synchronized void setNewChildren(List<String> newChildren)
    {
        if ( newChildren != null )
        {
            Data currentData = children.get();

            children.set(new Data(newChildren, currentData.version + 1));
            notifyFromCallback();
        }
    }
}
