package org.apache.curator.x.async;

import org.apache.curator.framework.api.transaction.CuratorOp;

public interface AsyncTransactionDeleteBuilder extends
    AsyncPathable<CuratorOp>
{
    AsyncPathable<CuratorOp> withVersion(int version);
}
