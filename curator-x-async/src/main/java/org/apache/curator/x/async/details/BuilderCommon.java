package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.Backgrounding;

class BuilderCommon<T>
{
    final InternalCallback<T> internalCallback;
    final Backgrounding backgrounding;
    final InternalWatcher watcher;

    BuilderCommon(UnhandledErrorListener unhandledErrorListener, boolean watched, BackgroundProc<T> proc)
    {
        watcher = watched ? new InternalWatcher() : null;
        internalCallback = new InternalCallback<>(proc, watcher);
        backgrounding = new Backgrounding(internalCallback, unhandledErrorListener);
    }
}
