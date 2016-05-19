package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.BlockingQueue;

public class TestPathChildrenCacheEventOrdering extends TestEventOrdering<PathChildrenCache>
{
    @Override
    protected int getActualQty(PathChildrenCache cache)
    {
        return cache.getCurrentData().size();
    }

    @Override
    protected PathChildrenCache newCache(CuratorFramework client, String path, final BlockingQueue<Event> events) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(client, path, false);
        PathChildrenCacheListener listener = new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    events.add(new Event(EventType.ADDED, event.getData().getPath()));
                }
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                {
                    events.add(new Event(EventType.DELETED, event.getData().getPath()));
                }
            }
        };
        cache.getListenable().addListener(listener);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        return cache;
    }
}
