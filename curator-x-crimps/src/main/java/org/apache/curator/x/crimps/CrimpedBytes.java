package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import java.util.concurrent.CompletionStage;

public interface CrimpedBytes<T> extends
    ErrorListenerPathAndBytesable<CompletionStage<T>>,
    PathAndBytesable<CompletionStage<T>>
{
}
