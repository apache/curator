package org.apache.curator.x.crimps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

class AsyncPathAndBytesable<T> implements Supplier<T>, BackgroundCallback
{
    private final Function<CuratorEvent, CrimpResult<T>> resultFunction;
    private final BlockingQueue<CrimpResult<T>> queue = new ArrayBlockingQueue<>(1);

    AsyncPathAndBytesable(Function<CuratorEvent, CrimpResult<T>> resultFunction)
    {
        this.resultFunction = resultFunction;
    }

    @Override
    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
    {
        queue.offer(resultFunction.apply(event));
    }

    @Override
    public T get()
    {
        try
        {
            CrimpResult<T> result = queue.take();
            if ( result.exception != null )
            {
                throw new CrimpException(result.exception);
            }
            return result.value;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new CrimpException(e);
        }
    }
}
