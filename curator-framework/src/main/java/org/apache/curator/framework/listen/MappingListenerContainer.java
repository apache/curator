package org.apache.curator.framework.listen;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Upgraded version of {@link org.apache.curator.framework.listen.ListenerContainer} that
 * doesn't leak Guava's internals and also supports mapping/wrapping of listeners
 */
public class MappingListenerContainer<K, V> implements Listenable<K>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<K, ListenerEntry<V>> listeners = Maps.newConcurrentMap();
    private final Function<K, V> mapper;

    /**
     * Returns a new standard version that does no mapping
     *
     * @return new container
     */
    public static <T> MappingListenerContainer<T, T> nonMapping()
    {
        return new MappingListenerContainer<>(Function.identity());
    }

    /**
     * Returns a new container that wraps listeners using the given mapper
     *
     * @param mapper listener mapper/wrapper
     * @return new container
     */
    public static <K, V> MappingListenerContainer<K, V> mapping(Function<K, V> mapper)
    {
        return new MappingListenerContainer<>(mapper);
    }

    @Override
    public void addListener(K listener)
    {
        addListener(listener, MoreExecutors.directExecutor());
    }

    @Override
    public void addListener(K listener, Executor executor)
    {
        V mapped = mapper.apply(listener);
        listeners.put(listener, new ListenerEntry<V>(mapped, executor));
    }

    @Override
    public void removeListener(K listener)
    {
        if ( listener != null )
        {
            listeners.remove(listener);
        }
    }

    /**
     * Remove all listeners
     */
    public void clear()
    {
        listeners.clear();
    }

    /**
     * Return the number of listeners
     *
     * @return number
     */
    public int size()
    {
        return listeners.size();
    }

    /**
     * Utility - apply the given function to each listener. The function receives
     * the listener as an argument.
     *
     * @param function function to call for each listener
     */
    public void forEach(Consumer<V> function)
    {
        for ( ListenerEntry<V> entry : listeners.values() )
        {
            entry.executor.execute(() -> {
                try
                {
                    function.accept(entry.listener);
                }
                catch ( Throwable e )
                {
                    ThreadUtils.checkInterrupted(e);
                    log.error(String.format("Listener (%s) threw an exception", entry.listener), e);
                }
            });
        }
    }

    private MappingListenerContainer(Function<K, V> mapper)
    {
        this.mapper = mapper;
    }
}
