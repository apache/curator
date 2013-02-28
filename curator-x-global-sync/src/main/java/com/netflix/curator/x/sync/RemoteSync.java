package com.netflix.curator.x.sync;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;

class RemoteSync implements Runnable
{
    private final CuratorGlobalSync sync;
    private final CuratorFramework remoteClient;
    private final ChildData data;

    RemoteSync(CuratorGlobalSync sync, CuratorFramework remoteClient, ChildData data)
    {
        this.sync = sync;
        this.remoteClient = remoteClient;
        this.data = data;
    }

    @Override
    public void run()
    {
        try
        {
            // TODO - is version enough? should I use timestamp instead?
            Stat stat = remoteClient.checkExists().forPath(data.getPath());
            if ( stat.getMtime() < data.getStat().getMtime() )
            {
                remoteClient.setData().withVersion(stat.getVersion()).forPath(data.getPath(), data.getData());
            }
        }
        catch ( Exception e )
        {
            // TODO - no node? bad version? connection issues?
        }
    }
}
