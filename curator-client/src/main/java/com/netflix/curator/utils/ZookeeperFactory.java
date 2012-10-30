package com.netflix.curator.utils;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public interface ZookeeperFactory
{
    /**
     * Allocate a new ZooKeeper instance
     *
     *
     * @param connectString the connection string
     * @param sessionTimeout session timeout in milliseconds
     * @param watcher optional watcher
     * @param canBeReadOnly if true, allow ZooKeeper client to enter
     *                      read only mode in case of a network partition. See
     *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     *                      for details
     * @return the instance
     * @throws Exception errors
     */
    public ZooKeeper        newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception;
}
