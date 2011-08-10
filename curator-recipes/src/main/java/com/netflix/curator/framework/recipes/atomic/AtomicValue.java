package com.netflix.curator.framework.recipes.atomic;

/**
 * Abstracts a value returned from one of the Atomics
 */
public interface AtomicValue<T>
{
    /**
     * <b>MUST be checked.</b> Returns true if the operation succeeded. If false is returned,
     * the operation failed and the atomic was not updated.
     *
     * @return true/false
     */
    public boolean      succeeded();

    /**
     * Returns the value of the counter prior to the operation
     *
     * @return pre-operation value
     */
    public T            preValue();

    /**
     * Returns the value of the counter after to the operation
     *
     * @return post-operation value
     */
    public T            postValue();

    /**
     * Returns debugging stats about the operation
     *
     * @return stats
     */
    public AtomicStats  getStats();
}
