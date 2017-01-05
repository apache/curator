package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.Pathable;
import java.util.concurrent.CompletionStage;

public interface CrimpedPathable<T> extends Pathable<CompletionStage<T>>
{
}
