package com.netflix.curator.framework.recipes.atomic;

public interface AtomicCounter<T>
{
    /**
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>     get() throws Exception;

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    increment() throws Exception;

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    decrement() throws Exception;

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    add(T delta) throws Exception;

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    subtract(T delta) throws Exception;
}
