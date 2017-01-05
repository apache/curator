package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import java.util.concurrent.CompletableFuture;

class CrimpedBackgroundCallback<T> extends CompletableFuture<T> implements BackgroundCallback
{
    private final BackgroundProc<T> resultFunction;

    CrimpedBackgroundCallback(BackgroundProc<T> resultFunction)
    {
        this.resultFunction = resultFunction;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        resultFunction.apply(event, this);
    }
}
