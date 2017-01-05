package org.apache.curator.x.crimps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class AsyncPathAndBytesable<T> extends CompletableFuture<T> implements BackgroundCallback
{
    private final Function<CuratorEvent, CrimpResult<T>> resultFunction;

    AsyncPathAndBytesable(Function<CuratorEvent, CrimpResult<T>> resultFunction)
    {
        this.resultFunction = resultFunction;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        CrimpResult<T> result = resultFunction.apply(event);
        if ( result.exception != null )
        {
            completeExceptionally(result.exception);
        }
        else
        {
            complete(result.value);
        }
    }
}
