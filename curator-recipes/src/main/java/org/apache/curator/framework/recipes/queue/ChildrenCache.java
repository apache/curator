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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.KeeperException;
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
                sync(true);
            }
        }
    };

    private final BackgroundCallback  callback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
            {
                setNewChildren(event.getChildren());
            }
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
        sync(true);
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

    private synchronized void notifyFromCallback()
    {
        notifyAll();
    }

    private synchronized void sync(boolean watched) throws Exception
    {
        if ( watched )
        {
            client.getChildren().usingWatcher(watcher).inBackground(callback).forPath(path);
        }
        else
        {
            client.getChildren().inBackground(callback).forPath(path);
        }
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
