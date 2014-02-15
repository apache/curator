package org.apache.curator.x.rest.api;

import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

class RestWatcher implements Watcher
{
    private final CuratorRestContext context;
    private final String watchId;

    RestWatcher(CuratorRestContext context, String watchId)
    {
        this.context = context;
        this.watchId = watchId;
    }

    @Override
    public void process(WatchedEvent event)
    {
        if ( event.getType() != Event.EventType.None )
        {
            context.pushMessage(new StatusMessage(Constants.WATCH, watchId, event.getType().name(), String.valueOf(event.getPath())));
        }
    }
}
