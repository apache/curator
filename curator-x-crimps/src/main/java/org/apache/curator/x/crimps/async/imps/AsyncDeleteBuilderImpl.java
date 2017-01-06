package org.apache.curator.x.crimps.async.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.BackgroundVersionable;
import org.apache.curator.framework.api.ChildrenDeletable;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.DeleteBuilderMain;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.crimps.async.AsyncDeleteBuilder;
import org.apache.curator.x.crimps.async.AsyncPathable;
import org.apache.curator.x.crimps.async.DeleteOption;
import org.apache.curator.x.crimps.async.details.AsyncCrimps;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

class AsyncDeleteBuilderImpl implements AsyncDeleteBuilder
{
    private final CuratorFramework client;
    private final AsyncCrimps async;
    private final UnhandledErrorListener unhandledErrorListener;
    private Set<DeleteOption> options = Collections.emptySet();
    private int version = -1;

    public AsyncDeleteBuilderImpl(CuratorFramework client, AsyncCrimps async, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.async = async;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncPathable<CompletionStage<Void>> withOptions(Set<DeleteOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        return this;
    }

    @Override
    public AsyncPathable<CompletionStage<Void>> withOptionsAndVersion(Set<DeleteOption> options, int version)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.version = version;
        return this;
    }

    @Override
    public AsyncPathable<CompletionStage<Void>> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    @Override
    public CompletionStage<Void> forPath(String path)
    {
        DeleteBuilder builder1 = client.delete();
        DeleteBuilderMain builder2 = options.contains(DeleteOption.quietly) ? builder1.quietly() : builder1;
        ChildrenDeletable builder3 = options.contains(DeleteOption.guaranteed) ? builder2.guaranteed() : builder2;
        BackgroundVersionable builder4 = options.contains(DeleteOption.deletingChildrenIfNeeded) ? builder3.deletingChildrenIfNeeded() : builder3;
        BackgroundPathable<Void> builder5 = (version >= 0) ? builder4.withVersion(version) : builder4;
        return async.ignored(builder5).forPath(path);
    }
}
