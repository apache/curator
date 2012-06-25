package com.netflix.curator.framework.imps;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class NamespaceFacadeCache
{
    private final CuratorFrameworkImpl                  client;
    private final NamespaceFacade                       nullNamespace;
    private final CacheLoader<String, NamespaceFacade>  loader = new CacheLoader<String, NamespaceFacade>()
    {
        @Override
        public NamespaceFacade load(String namespace) throws Exception
        {
            return new NamespaceFacade(client, namespace);
        }
    };
    private final LoadingCache<String, NamespaceFacade> cache = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES) // does this need config? probably not
        .build(loader);

    NamespaceFacadeCache(CuratorFrameworkImpl client)
    {
        this.client = client;
        nullNamespace = new NamespaceFacade(client, null);
    }

    NamespaceFacade     get(String namespace)
    {
        try
        {
            return (namespace != null) ? cache.get(namespace) : nullNamespace;
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException(e);  // should never happen
        }
    }
}
