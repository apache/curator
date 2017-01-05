package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletionStage;

class CrimpedBytesImpl<T> implements CrimpedBytes<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BackgroundPathAndBytesable<T> builder;
    private final BackgroundProc<T> resultFunction;
    private UnhandledErrorListener unhandledErrorListener;

    CrimpedBytesImpl(BackgroundPathAndBytesable<T> builder, BackgroundProc<T> resultFunction)
    {
        this.builder = builder;
        this.resultFunction = resultFunction;
        unhandledErrorListener = null;
    }

    @Override
    public PathAndBytesable<CompletionStage<T>> withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        unhandledErrorListener = listener;
        return this;
    }

    @Override
    public CompletionStage<T> forPath(String path) throws Exception
    {
        return forPath(path, null);
    }

    @Override
    public CompletionStage<T> forPath(String path, byte[] data) throws Exception
    {
        AsyncPathAndBytesable<T> supplier = new AsyncPathAndBytesable<T>(resultFunction);
        ErrorListenerPathAndBytesable<T> localBuilder = builder.inBackground(supplier);
        PathAndBytesable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        finalLocalBuilder.forPath(path, data);
        return supplier;
    }
}
