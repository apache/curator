package org.apache.curator.x.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionOp;

/**
 * Zookeeper framework-style client
 */
public interface AsyncCuratorFramework extends AsyncCuratorFrameworkDsl
{
    CuratorFramework getCuratorFramework();

    WatchedAsyncCuratorFramework watched();

    AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener);

    /**
     * Allocate an operation that can be used with {@link #transaction()}.
     * NOTE: {@link CuratorOp} instances created by this builder are
     * reusable.
     *
     * @return operation builder
     */
    TransactionOp transactionOp();  // TODO - versions that don't throw
}
