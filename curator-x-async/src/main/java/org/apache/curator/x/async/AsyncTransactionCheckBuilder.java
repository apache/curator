package org.apache.curator.x.async;

import org.apache.curator.framework.api.transaction.CuratorOp;

public interface AsyncTransactionCheckBuilder extends
    AsyncPathable<CuratorOp>
{
    /**
     * Use the given version (the default is -1)
     *
     * @param version version to use
     * @return this
     */
    AsyncPathable<CuratorOp> withVersion(int version);
}
