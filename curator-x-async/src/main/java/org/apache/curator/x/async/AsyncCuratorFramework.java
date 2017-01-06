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
    CuratorFramework unwrap();

    WatchedAsyncCuratorFramework watched();

    AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener);
}
