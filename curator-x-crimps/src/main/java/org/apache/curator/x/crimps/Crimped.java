package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.Backgroundable;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.Pathable;
import java.util.concurrent.CompletableFuture;

public interface Crimped<T> extends
    ErrorListenerPathable<CompletableFuture<T>>,
    Pathable<CompletableFuture<T>>
{
}
