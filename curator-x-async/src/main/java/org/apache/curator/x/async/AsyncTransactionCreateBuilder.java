package org.apache.curator.x.async;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;

public interface AsyncTransactionCreateBuilder extends AsyncPathAndBytesable<CuratorOp>
{
    AsyncPathable<CuratorOp> withMode(CreateMode createMode);

    AsyncPathable<CuratorOp> withACL(List<ACL> aclList);

    AsyncPathable<CuratorOp> compressed();

    AsyncPathable<CuratorOp> withOptions(CreateMode createMode, List<ACL> aclList, boolean compressed);
}
