package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.RemoveWatchesBuilderImpl;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncRemoveWatchesBuilder;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.RemoveWatcherOption;
import org.apache.zookeeper.Watcher;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncRemoveWatchesBuilderImpl implements AsyncRemoveWatchesBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private Watcher.WatcherType watcherType = Watcher.WatcherType.Any;
    private Set<RemoveWatcherOption> options = Collections.emptySet();
    private Watcher watcher = null;
    private CuratorWatcher curatorWatcher = null;

    AsyncRemoveWatchesBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll()
    {
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Set<RemoveWatcherOption> options)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Set<RemoveWatcherOption> options)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Set<RemoveWatcherOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType)
    {
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncStage<Void> forPath(String path)
    {
        BuilderCommon<Void> common = new BuilderCommon<>(unhandledErrorListener, false, ignoredProc);
        RemoveWatchesBuilderImpl builder = new RemoveWatchesBuilderImpl(client,
            watcher,
            curatorWatcher,
            watcherType,
            options.contains(RemoveWatcherOption.guaranteed),
            options.contains(RemoveWatcherOption.local),
            options.contains(RemoveWatcherOption.guaranteed),
            common.backgrounding
        );
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
