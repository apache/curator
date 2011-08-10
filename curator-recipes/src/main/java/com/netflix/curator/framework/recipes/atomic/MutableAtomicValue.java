package com.netflix.curator.framework.recipes.atomic;

class MutableAtomicValue<T> implements AtomicValue<T>
{
    T preValue;
    T postValue;
    boolean succeeded = false;
    AtomicStats stats = new AtomicStats();

    MutableAtomicValue(T preValue, T postValue)
    {
        this(preValue, postValue, false);
    }

    MutableAtomicValue(T preValue, T postValue, boolean succeeded)
    {
        this.preValue = preValue;
        this.postValue = postValue;
        this.succeeded = succeeded;
    }

    @Override
    public T preValue()
    {
        return preValue;
    }

    @Override
    public T postValue()
    {
        return postValue;
    }

    @Override
    public boolean succeeded()
    {
        return succeeded;
    }

    @Override
    public AtomicStats getStats()
    {
        return stats;
    }
}
