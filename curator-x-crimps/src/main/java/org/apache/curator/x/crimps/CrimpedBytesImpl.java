package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

class CrimpedBytesImpl<T> implements CrimpedBytes<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BackgroundPathAndBytesable<T> builder;
    private final Function<CuratorEvent, CrimpResult<T>> resultFunction;
    private final Executor executor;
    private UnhandledErrorListener unhandledErrorListener;

    CrimpedBytesImpl(BackgroundPathAndBytesable<T> builder, Executor executor, Function<CuratorEvent, CrimpResult<T>> resultFunction)
    {
        this.executor = executor;
        this.builder = builder;
        this.resultFunction = resultFunction;
        unhandledErrorListener = null;
    }

    @Override
    public PathAndBytesable<CompletableFuture<T>> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        unhandledErrorListener = listener;
        return this;
    }

    @Override
    public CompletableFuture<T> forPath(String path) throws Exception
    {
        return forPath(path, null);
    }

    @Override
    public CompletableFuture<T> forPath(String path, byte[] data) throws Exception
    {
        AsyncPathAndBytesable<T> supplier = new AsyncPathAndBytesable<T>(resultFunction);
        ErrorListenerPathAndBytesable<T> localBuilder = builder.inBackground(supplier);
        PathAndBytesable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        finalLocalBuilder.forPath(path, data);
        return CompletableFuture.supplyAsync(supplier, executor);
    }
}
