package org.apache.curator.x.async;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public interface AsyncExistsBuilder
    extends AsyncPathable<AsyncStage<Stat>>
{
    /**
     * Causes any parent nodes to get created if they haven't already been
     *
     * @return this
     */
    AsyncPathable<AsyncStage<Stat>> creatingParentsIfNeeded();

    /**
     * Causes any parent nodes to get created using {@link CreateMode#CONTAINER} if they haven't already been.
     * IMPORTANT NOTE: container creation is a new feature in recent versions of ZooKeeper.
     * If the ZooKeeper version you're using does not support containers, the parent nodes
     * are created as ordinary PERSISTENT nodes.
     *
     * @return this
     */
    AsyncPathable<AsyncStage<Stat>> creatingParentContainersIfNeeded();
}
