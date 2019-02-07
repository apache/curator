package org.apache.curator.framework.state;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.ScheduledExecutorService;

/**
 * <p>
 *     Allows for the enhancement of the {@link org.apache.curator.framework.state.ConnectionStateListener} instances
 *     used with Curator. Client code that sets a ConnectionStateListener should always wrap it using the configured
 *     ConnectionStateListenerDecorator. All Curator recipes do this.
 * </p>
 *
 * <p>
 *     E.g.
 *
 * <code><pre>
 * CuratorFramework client ...
 * ConnectionStateListener listener = ...
 * ConnectionStateListener decoratedListener = client.decorateConnectionStateListener(listener);
 *
 * ...
 *
 * client.getConnectionStateListenable().addListener(decoratedListener);
 *
 * // later, to remove...
 * client.getConnectionStateListenable().removeListener(decoratedListener);
 * </pre></code>
 * </p>
 */
@FunctionalInterface
public interface ConnectionStateListenerDecorator
{
    ConnectionStateListener decorateListener(CuratorFramework client, ConnectionStateListener actual);

    /**
     * Pass through - does no decoration
     */
    ConnectionStateListenerDecorator standard = (__, actual) -> actual;

    /**
     * Decorates the listener with circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     *
     * @param retryPolicy the circuit breaking policy to use
     * @return new decorator
     */
    static ConnectionStateListenerDecorator circuitBreaking(RetryPolicy retryPolicy)
    {
        return (client, actual) -> new CircuitBreakingConnectionStateListener(client, actual, retryPolicy);
    }

    /**
     * Decorates the listener with circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     *
     * @param retryPolicy the circuit breaking policy to use
     * @param service the scheduler to use
     * @return new decorator
     */
    static ConnectionStateListenerDecorator circuitBreaking(RetryPolicy retryPolicy, ScheduledExecutorService service)
    {
        return (client, actual) -> new CircuitBreakingConnectionStateListener(client, actual, retryPolicy, service);
    }
}
