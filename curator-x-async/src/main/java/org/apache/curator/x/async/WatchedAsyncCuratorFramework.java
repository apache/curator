package org.apache.curator.x.async;

/**
 * Zookeeper framework-style client
 */
public interface WatchedAsyncCuratorFramework
{
    /**
     * Start an exists builder
     * <p>
     * The builder will return a Stat object as if org.apache.zookeeper.ZooKeeper.exists() were called.  Thus, a null
     * means that it does not exist and an actual Stat object means it does exist.
     *
     * @return builder object
     */
    AsyncExistsBuilder checkExists();

    /**
     * Start a get data builder
     *
     * @return builder object
     */
    AsyncGetDataBuilder getData();

    /**
     * Start a get children builder
     *
     * @return builder object
     */
    AsyncGetChildrenBuilder getChildren();

    /**
     * Start a getConfig builder
     *
     * @return builder object
     */
    AsyncGetConfigBuilder getConfig();
}
