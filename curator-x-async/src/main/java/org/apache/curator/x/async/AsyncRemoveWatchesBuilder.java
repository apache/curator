package org.apache.curator.x.async;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.Watcher;
import java.util.Set;

public interface AsyncRemoveWatchesBuilder extends
    AsyncPathable<AsyncStage<Void>>
{
    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher);

    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher);

    AsyncPathable<AsyncStage<Void>> removingAll();

    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removingAll(Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options);

    AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType);

    AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType);

    AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType);
}
