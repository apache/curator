/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ChildrenCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final String path;
    private final ExecutorService service;
    private final AtomicReference<Data> children = new AtomicReference<Data>(new Data(Lists.<String>newArrayList(), 0));
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            notifyFromCallback();
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

    ChildrenCache(CuratorFramework client, String path, ExecutorService service)
    {
        this.client = client;
        this.path = path;
        this.service = service;
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
                    while ( !Thread.currentThread().isInterrupted() )
                    {
                        sync();
                    }
                }
            }
        );
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

    synchronized void sync()
    {
        try
        {
            List<String> newChildren = client.getChildren().usingWatcher(watcher).forPath(path);
            Data currentData = children.get();

            children.set(new Data(newChildren, currentData.version + 1));
            notifyFromCallback();

            wait();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        catch ( KeeperException ignore )
        {
            // ignore
        }
        catch ( Exception e )
        {
            log.error("Syncing for queue", e);
        }
    }

    protected synchronized void notifyFromCallback()
    {
        notifyAll();
    }
}
