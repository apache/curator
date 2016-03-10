package org.apache.curator.framework.api;

import org.apache.curator.framework.api.transaction.CuratorMultiTransactionMain;

public interface ErrorListenerMultiTransactionMain<T> extends CuratorMultiTransactionMain
{
    /**
     * Set an error listener for this background operation. If an exception
     * occurs while processing the call in the background, this listener will
     * be called
     *
     * @param listener the listener
     * @return this for chaining
     */
    CuratorMultiTransactionMain withUnhandledErrorListener(UnhandledErrorListener listener);
}
