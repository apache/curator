package org.apache.curator.framework.state;

/**
 * Recipes should use the configured error policy to decide how to handle
 * errors such as {@link ConnectionState} changes.
 *
 * @since 3.0.0
 */
public interface ErrorPolicy
{
    /**
     * Returns true if the given state should cause the recipe to
     * act as though the connection has been lost. i.e. locks should
     * exit, etc.
     *
     * @param state the state
     * @return true/false
     */
    boolean isErrorState(ConnectionState state);
}
