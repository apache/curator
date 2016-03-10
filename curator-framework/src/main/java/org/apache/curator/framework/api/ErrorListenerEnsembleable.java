package org.apache.curator.framework.api;

public interface ErrorListenerEnsembleable<T> extends Ensembleable<T>
{
    /**
     * Set an error listener for this background operation. If an exception
     * occurs while processing the call in the background, this listener will
     * be called
     *
     * @param listener the listener
     * @return this for chaining
     */
    Ensembleable<T> withUnhandledErrorListener(UnhandledErrorListener listener);
}
