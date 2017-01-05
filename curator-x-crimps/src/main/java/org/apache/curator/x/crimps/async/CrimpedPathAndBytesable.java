package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.PathAndBytesable;
import java.util.concurrent.CompletionStage;

public interface CrimpedPathAndBytesable<T> extends PathAndBytesable<CompletionStage<T>>
{
}
