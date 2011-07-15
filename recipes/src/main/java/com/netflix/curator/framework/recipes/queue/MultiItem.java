package com.netflix.curator.framework.recipes.queue;

public interface MultiItem<T>
{
    public T    nextItem() throws Exception;
}
