package com.netflix.curator.utils;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DefaultZookeeperFactory implements ZookeeperFactory
{
    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
    {
        return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
    }
}
