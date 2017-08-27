package org.apache.curator.framework.recipes.watch;

import org.apache.zookeeper.data.Stat;

public interface CachedNode
{
    Stat getStat();

    byte[] getData();
}
