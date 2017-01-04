package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import java.util.concurrent.CompletableFuture;

public interface CrimpedBytes<T> extends
    ErrorListenerPathAndBytesable<CompletableFuture<T>>,
    PathAndBytesable<CompletableFuture<T>>
{
}
