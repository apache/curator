package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletionStage;

class CrimpedImpl<T> implements Crimped<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BackgroundPathable<T> builder;
    private final BackgroundProc<T> resultFunction;
    private UnhandledErrorListener unhandledErrorListener;

    CrimpedImpl(BackgroundPathable<T> builder, BackgroundProc<T> resultFunction)
    {
        this.builder = builder;
        this.resultFunction = resultFunction;
        unhandledErrorListener = null;
    }

    @Override
    public Pathable<CompletionStage<T>> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        unhandledErrorListener = listener;
        return this;
    }

    @Override
    public CompletionStage<T> forPath(String path) throws Exception
    {
        AsyncPathAndBytesable<T> supplier = new AsyncPathAndBytesable<T>(resultFunction);
        ErrorListenerPathable<T> localBuilder = builder.inBackground(supplier);
        Pathable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        finalLocalBuilder.forPath(path);
        return supplier;
    }
}
