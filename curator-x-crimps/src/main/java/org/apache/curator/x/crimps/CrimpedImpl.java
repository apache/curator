package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

class CrimpedImpl<T> implements Crimped<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BackgroundPathable<T> builder;
    private final Function<CuratorEvent, CrimpResult<T>> resultFunction;
    private final Executor executor;
    private UnhandledErrorListener unhandledErrorListener;

    CrimpedImpl(BackgroundPathable<T> builder, Executor executor, Function<CuratorEvent, CrimpResult<T>> resultFunction)
    {
        this.executor = executor;
        this.builder = builder;
        this.resultFunction = resultFunction;
        unhandledErrorListener = null;
    }

    @Override
    public Pathable<CompletableFuture<T>> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        unhandledErrorListener = listener;
        return this;
    }

    @Override
    public CompletableFuture<T> forPath(String path) throws Exception
    {
        AsyncPathAndBytesable<T> supplier = new AsyncPathAndBytesable<T>(resultFunction);
        ErrorListenerPathable<T> localBuilder = builder.inBackground(supplier);
        Pathable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        finalLocalBuilder.forPath(path);
        return CompletableFuture.supplyAsync(supplier, executor);
    }
}
