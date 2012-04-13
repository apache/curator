package com.netflix.curator.utils;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public interface ZookeeperFactory
{
    public ZooKeeper        newZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws Exception;
}
