package org.apache.curator.x.async;

import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.curator.framework.api.RemoveWatchesBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.transaction.CuratorOp;

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
    AsyncCreateBuilder create();

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
    AsyncReconfigBuilder reconfig();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    AsyncMultiTransaction transaction();

    /**
     * Allocate an operation that can be used with {@link #transaction()}.
     * NOTE: {@link CuratorOp} instances created by this builder are
     * reusable.
     *
     * @return operation builder
     */
    AsyncTransactionOp transactionOp();

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    AsyncSyncBuilder sync();

    /**
     * Start a remove watches builder.
     * @return builder object
     */
    AsyncRemoveWatchesBuilder watches();
}
