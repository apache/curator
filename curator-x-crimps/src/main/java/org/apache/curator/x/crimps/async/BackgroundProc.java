package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.CuratorEvent;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

interface BackgroundProc<T> extends BiFunction<CuratorEvent, CompletableFuture<T>, Void>
{
}
