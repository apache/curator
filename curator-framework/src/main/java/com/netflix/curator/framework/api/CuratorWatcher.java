package com.netflix.curator.framework.api;

import org.apache.zookeeper.WatchedEvent;

public interface CuratorWatcher
{
    public void process(WatchedEvent event) throws Exception;
}
