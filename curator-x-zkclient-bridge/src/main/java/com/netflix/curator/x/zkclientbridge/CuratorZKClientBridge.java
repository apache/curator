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

package com.netflix.curator.x.zkclientbridge;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorListener;
import org.I0Itec.zkclient.IZkConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import java.util.List;

/**
 * <p>
 *     Bridge between ZKClient and Curator. Accomplished via an implementation for
 *     {@link IZkConnection} which is the abstraction ZKClient uses to wrap the raw ZooKeeper handle
 * </p>
 *
 * <p>
 *     Once allocated, bridge to ZKClient via:
 *     <code><pre>
 *     ZKClient zkClient = new ZkClient(new CuratorZKClientBridge(curatorInstance, timeout));
 *     </pre></code>
 * </p>
 */
public class CuratorZKClientBridge implements IZkConnection
{
    private final CuratorFramework curator;
    private final boolean doClose;

    /**
     * @param curator Curator instance to bridge
     */
    public CuratorZKClientBridge(CuratorFramework curator)
    {
        this(curator, true);
    }

    /**
     * @param curator Curator instance to bridge
     * @param doClose if true {@link #close()} will close the curator instance. Otherwise it's a NOP.
     */
    public CuratorZKClientBridge(CuratorFramework curator, boolean doClose)
    {
        this.curator = curator;
        this.doClose = doClose;
    }

    /**
     * Return the client
     *
     * @return client
     */
    public CuratorFramework getCurator()
    {
        return curator;
    }

    @Override
    public void connect(final Watcher watcher)
    {
        if ( watcher != null )
        {
            curator.getCuratorListenable().addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        if ( event.getWatchedEvent() != null )
                        {
                            watcher.process(event.getWatchedEvent());
                        }
                    }
                }
            );

            try
            {
                BackgroundCallback      callback = new BackgroundCallback()
                {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        WatchedEvent        fakeEvent = new WatchedEvent(Watcher.Event.EventType.None, curator.getZookeeperClient().isConnected() ? Watcher.Event.KeeperState.SyncConnected : Watcher.Event.KeeperState.Disconnected, null);
                        watcher.process(fakeEvent);
                    }
                };
                curator.checkExists().inBackground(callback).forPath("/");
            }
            catch ( Exception e )
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws InterruptedException
    {
        if ( doClose )
        {
            curator.close();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException
    {
        try
        {
            return curator.create().withMode(mode).forPath(path, data);
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
        return null;    // will never execute
    }

    @Override
    public void delete(String path) throws InterruptedException, KeeperException
    {
        try
        {
            curator.delete().forPath(path);
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
    }

    @Override
    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException
    {
        try
        {
            return watch ? (curator.checkExists().watched().forPath(path) != null) : (curator.checkExists().forPath(path) != null);
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
        return false;   // will never execute
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException
    {
        try
        {
            return watch ? curator.getChildren().watched().forPath(path) : curator.getChildren().forPath(path);
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
        return null;   // will never execute
    }

    @Override
    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException
    {
        try
        {
            if ( stat != null )
            {
                return watch ? curator.getData().storingStatIn(stat).watched().forPath(path) : curator.getData().storingStatIn(stat).forPath(path);
            }
            else
            {
                return watch ? curator.getData().watched().forPath(path) : curator.getData().forPath(path);
            }
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
        return null;   // will never execute
    }

    @Override
    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException
    {
        try
        {
            curator.setData().withVersion(expectedVersion).forPath(path, data);
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
    }

    @Override
    public ZooKeeper.States getZookeeperState()
    {
        try
        {
            return curator.getZookeeperClient().getZooKeeper().getState();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getCreateTime(String path) throws KeeperException, InterruptedException
    {
        try
        {
            Stat            stat = curator.checkExists().forPath(path);
            return (stat != null) ? stat.getCtime() : 0;
        }
        catch ( Exception e )
        {
            adjustException(e);
        }
        return 0;
    }

    @Override
    public String getServers()
    {
        return curator.getZookeeperClient().getCurrentConnectionString();
    }

    private void adjustException(Exception e) throws KeeperException, InterruptedException
    {
        if ( e instanceof KeeperException )
        {
            throw (KeeperException)e;
        }

        if ( e instanceof InterruptedException )
        {
            throw (InterruptedException)e;
        }

        throw new RuntimeException(e);
    }
}
