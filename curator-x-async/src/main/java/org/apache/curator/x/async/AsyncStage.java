package org.apache.curator.x.async;

import org.apache.zookeeper.WatchedEvent;
import java.util.concurrent.CompletionStage;

public interface AsyncStage<T> extends CompletionStage<T>
{
    CompletionStage<WatchedEvent> event();
}
