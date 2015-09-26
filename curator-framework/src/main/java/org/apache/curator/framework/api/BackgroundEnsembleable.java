package org.apache.curator.framework.api;

public interface BackgroundEnsembleable<T> extends
    Backgroundable<Ensembleable<T>>,
    Ensembleable<T>
{
}
