package org.apache.curator.x.async;

import org.apache.curator.framework.api.transaction.CuratorOp;

public interface AsyncTransactionSetDataBuilder extends
    AsyncPathAndBytesable<CuratorOp>
{
    AsyncPathAndBytesable<CuratorOp> withVersion(int version);

    AsyncPathAndBytesable<CuratorOp> compressed();

    AsyncPathAndBytesable<CuratorOp> withVersionCompressed(int version);
}
