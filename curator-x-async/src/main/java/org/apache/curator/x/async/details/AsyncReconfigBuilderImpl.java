package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.ReconfigBuilderImpl;
import org.apache.curator.x.async.AsyncEnsemblable;
import org.apache.curator.x.async.AsyncReconfigBuilder;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;
import java.util.List;

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncReconfigBuilderImpl implements AsyncReconfigBuilder, AsyncEnsemblable<AsyncStage<Void>>
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private Stat stat = null;
    private long fromConfig = -1;
    private List<String> newMembers = null;
    private List<String> joining = null;
    private List<String> leaving = null;

    AsyncReconfigBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers)
    {
        this.newMembers = servers;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving)
    {
        this.joining = joining;
        this.leaving = leaving;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat)
    {
        this.newMembers = servers;
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat)
    {
        this.joining = joining;
        this.leaving = leaving;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat, long fromConfig)
    {
        this.newMembers = servers;
        this.stat = stat;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat, long fromConfig)
    {
        this.joining = joining;
        this.leaving = leaving;
        this.stat = stat;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, long fromConfig)
    {
        this.newMembers = servers;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, long fromConfig)
    {
        this.joining = joining;
        this.leaving = leaving;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncStage<Void> forEnsemble()
    {
        BuilderCommon<Void> common = new BuilderCommon<>(unhandledErrorListener, false, ignoredProc);
        ReconfigBuilderImpl builder = new ReconfigBuilderImpl(client, common.backgrounding, stat, fromConfig, newMembers, joining, leaving);
        return safeCall(common.internalCallback, () -> {
            builder.forEnsemble();
            return null;
        });
    }
}
