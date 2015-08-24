package org.apache.curator.framework.state;

/**
 * This policy treats only {@link ConnectionState#LOST} as an error
 */
public class SessionErrorPolicy implements ErrorPolicy
{
    @Override
    public boolean isErrorState(ConnectionState state)
    {
        return state == ConnectionState.LOST;
    }
}
