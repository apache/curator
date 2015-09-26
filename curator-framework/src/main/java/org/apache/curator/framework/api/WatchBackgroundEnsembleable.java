package org.apache.curator.framework.api;

public interface WatchBackgroundEnsembleable<T> extends
    Watchable<BackgroundEnsembleable<T>>,
    BackgroundEnsembleable<T>
{
}
