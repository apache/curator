package org.apache.curator.framework.state;

/**
 * This policy treats {@link ConnectionState#SUSPENDED} and {@link ConnectionState#LOST}
 * as errors
 */
public class StandardConnectionStateErrorPolicy implements ConnectionStateErrorPolicy
{
    @Override
    public boolean isErrorState(ConnectionState state)
    {
        return ((state == ConnectionState.SUSPENDED) || (state == ConnectionState.LOST));
    }
}
