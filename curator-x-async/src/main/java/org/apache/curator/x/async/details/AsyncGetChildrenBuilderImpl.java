package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.GetChildrenBuilderImpl;
import org.apache.curator.x.async.AsyncGetChildrenBuilder;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;
import java.util.List;

import static org.apache.curator.x.async.details.BackgroundProcs.childrenProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncGetChildrenBuilderImpl implements AsyncGetChildrenBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private Stat stat = null;

    AsyncGetChildrenBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncStage<List<String>> forPath(String path)
    {
        BuilderCommon<List<String>> common = new BuilderCommon<>(unhandledErrorListener, watched, childrenProc);
        GetChildrenBuilderImpl builder = new GetChildrenBuilderImpl(client, common.watcher, common.backgrounding, stat);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }

    @Override
    public AsyncPathable<AsyncStage<List<String>>> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }
}
