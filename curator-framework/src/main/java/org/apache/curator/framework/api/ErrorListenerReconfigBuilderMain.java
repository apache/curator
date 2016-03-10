package org.apache.curator.framework.api;

public interface ErrorListenerReconfigBuilderMain extends ReconfigBuilderMain
{
    /**
     * Set an error listener for this background operation. If an exception
     * occurs while processing the call in the background, this listener will
     * be called
     *
     * @param listener the listener
     * @return this for chaining
     */
    ReconfigBuilderMain withUnhandledErrorListener(UnhandledErrorListener listener);
}
