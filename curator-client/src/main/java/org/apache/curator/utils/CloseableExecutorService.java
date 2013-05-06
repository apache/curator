package org.apache.curator.utils;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutorService;

/**
 * Decorates an {@link ExecutorService} such that submitted tasks
 * are recorded and can be closed en masse.
 */
public class CloseableExecutorService extends CloseableExecutorServiceBase
{
    private final ListeningExecutorService executorService;

    /**
     * @param executorService the service to decorate
     */
    public CloseableExecutorService(ExecutorService executorService)
    {
        this.executorService = MoreExecutors.listeningDecorator(executorService);
    }

    @Override
    protected ListeningExecutorService getService()
    {
        return executorService;
    }
}
