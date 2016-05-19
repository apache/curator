package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.BlockingQueue;

public class TestTreeCacheEventOrdering extends TestEventOrdering<TreeCache>
{
    @Override
    protected int getActualQty(TreeCache cache)
    {
        return cache.getCurrentChildren("/root").size();
    }

    @Override
    protected TreeCache newCache(CuratorFramework client, String path, final BlockingQueue<Event> events) throws Exception
    {
        TreeCache cache = new TreeCache(client, path);
        TreeCacheListener listener = new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
            {
                if ( (event.getData() != null) && (event.getData().getPath().startsWith("/root/")) )
                {
                    if ( event.getType() == TreeCacheEvent.Type.NODE_ADDED )
                    {
                        events.add(new Event(EventType.ADDED, event.getData().getPath()));
                    }
                    if ( event.getType() == TreeCacheEvent.Type.NODE_REMOVED )
                    {
                        events.add(new Event(EventType.DELETED, event.getData().getPath()));
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
        cache.start();
        return cache;
    }
}
