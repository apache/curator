package org.apache.curator.x.async;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Set;

public interface AsyncCreateBuilder extends
    AsyncPathAndBytesable<AsyncStage<String>>
{
    /**
     * Have the operation fill the provided stat object
     *
     * @param stat the stat to have filled in
     * @return this
     */
    AsyncPathable<AsyncStage<String>> storingStatIn(Stat stat);

    AsyncPathable<AsyncStage<String>> withMode(CreateMode createMode);

    AsyncPathable<AsyncStage<String>> withACL(List<ACL> aclList);

    AsyncPathable<AsyncStage<String>> withOptions(Set<CreateOption> options);

    AsyncPathable<AsyncStage<String>> withOptions(Set<CreateOption> options, List<ACL> aclList);

    AsyncPathable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList);

    AsyncPathable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode);

    AsyncPathable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList, Stat stat);
}
