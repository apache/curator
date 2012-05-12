package com.netflix.curator.framework.api;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A version of {@link Watcher} that can throw an exception
 */
public interface CuratorWatcher
{
    /**
     * Same as {@link Watcher#process(WatchedEvent)}. If an exception
     * is thrown, Curator will log it
     *
     * @param event the event
     * @throws Exception any exceptions to log
     */
    public void process(WatchedEvent event) throws Exception;
}
