package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.DeleteBuilderImpl;
import org.apache.curator.x.async.AsyncDeleteBuilder;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.DeleteOption;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncDeleteBuilderImpl implements AsyncDeleteBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private Set<DeleteOption> options = Collections.emptySet();
    private int version = -1;

    public AsyncDeleteBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withOptions(Set<DeleteOption> options)
    {
        return withOptionsAndVersion(options, -1);
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withOptionsAndVersion(Set<DeleteOption> options, int version)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.version = version;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    @Override
    public AsyncStage<Void> forPath(String path)
    {
        BuilderCommon<Void> common = new BuilderCommon<>(unhandledErrorListener, false, ignoredProc);
        DeleteBuilderImpl builder = new DeleteBuilderImpl(client, version, common.backgrounding, options.contains(DeleteOption.deletingChildrenIfNeeded), options.contains(DeleteOption.guaranteed), options.contains(DeleteOption.quietly));
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
