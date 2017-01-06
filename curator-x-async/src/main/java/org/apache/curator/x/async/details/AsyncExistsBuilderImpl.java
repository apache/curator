package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.ExistsBuilderImpl;
import org.apache.curator.x.async.AsyncExistsBuilder;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import static org.apache.curator.x.async.details.BackgroundProcs.safeStatProc;

class AsyncExistsBuilderImpl implements AsyncExistsBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private boolean createParentsIfNeeded = false;
    private boolean createParentContainersIfNeeded = false;

    AsyncExistsBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> creatingParentsIfNeeded()
    {
        createParentsIfNeeded = true;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> creatingParentContainersIfNeeded()
    {
        createParentContainersIfNeeded = true;
        return this;
    }

    @Override
    public AsyncStage<Stat> forPath(String path)
    {
        BuilderCommon<Stat> common = new BuilderCommon<>(unhandledErrorListener, watched, safeStatProc);
        ExistsBuilderImpl builder = new ExistsBuilderImpl(client, common.backgrounding, common.watcher, createParentsIfNeeded, createParentContainersIfNeeded);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
