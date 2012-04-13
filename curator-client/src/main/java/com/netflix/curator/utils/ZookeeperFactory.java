package com.netflix.curator.utils;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public interface ZookeeperFactory
{
    /**
     * Allocate a new ZooKeeper instance
     *
     * @param connectString the connection string
     * @param sessionTimeout session timeout in milliseconds
     * @param watcher optional watcher
     * @return the instance
     * @throws Exception errors
     */
    public ZooKeeper        newZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws Exception;
}
