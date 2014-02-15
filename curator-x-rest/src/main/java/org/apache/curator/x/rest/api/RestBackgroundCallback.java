package org.apache.curator.x.rest.api;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.StatusMessage;

class RestBackgroundCallback implements BackgroundCallback
{
    private final CuratorRestContext context;
    private final String type;
    private final String asyncId;

    RestBackgroundCallback(CuratorRestContext context, String type, String asyncId)
    {
        this.context = context;
        this.type = type;
        this.asyncId = asyncId;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        context.pushMessage(new StatusMessage(type, asyncId, getMessage(event), Integer.toString(event.getResultCode())));
    }

    protected String getMessage(CuratorEvent event)
    {
        return String.valueOf(event.getName());
    }
}
