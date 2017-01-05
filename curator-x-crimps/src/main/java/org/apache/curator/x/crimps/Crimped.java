package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.Pathable;
import java.util.concurrent.CompletionStage;

public interface Crimped<T> extends
    ErrorListenerPathable<CompletionStage<T>>,
    Pathable<CompletionStage<T>>
{
}
