package org.apache.curator.x.async;

import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.curator.framework.api.RemoveWatchesBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.SyncBuilder;

/**
 * Zookeeper framework-style client
 */
public interface AsyncCuratorFrameworkDsl extends WatchedAsyncCuratorFramework
{
    /**
     * Start a create builder
     *
     * @return builder object
     */
    CreateBuilder create();

    /**
     * Start a delete builder
     *
     * @return builder object
     */
    AsyncDeleteBuilder delete();

    /**
     * Start a set data builder
     *
     * @return builder object
     */
    AsyncSetDataBuilder setData();

    /**
     * Start a get ACL builder
     *
     * @return builder object
     */
    GetACLBuilder getACL();

    /**
     * Start a set ACL builder
     *
     * @return builder object
     */
    SetACLBuilder setACL();

    /**
     * Start a reconfig builder
     *
     * @return builder object
     */
    ReconfigBuilder reconfig();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    AsyncMultiTransaction transaction();

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    SyncBuilder sync();

    /**
     * Start a remove watches builder.
     * @return builder object
     */
    RemoveWatchesBuilder watches();
}
