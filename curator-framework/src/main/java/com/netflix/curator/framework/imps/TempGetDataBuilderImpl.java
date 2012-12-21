package com.netflix.curator.framework.imps;

import com.netflix.curator.RetryLoop;
import com.netflix.curator.TimeTrace;
import com.netflix.curator.framework.api.Pathable;
import com.netflix.curator.framework.api.StatPathable;
import com.netflix.curator.framework.api.TempGetDataBuilder;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.Callable;

class TempGetDataBuilderImpl implements TempGetDataBuilder
{
    private final CuratorFrameworkImpl  client;
    private Stat                        responseStat;
    private boolean                     decompress;

    TempGetDataBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
        responseStat = null;
        decompress = false;
    }

    @Override
    public StatPathable<byte[]> decompressed()
    {
        decompress = true;
        return this;
    }

    @Override
    public Pathable<byte[]> storingStatIn(Stat stat)
    {
        responseStat = stat;
        return this;
    }

    @Override
    public byte[] forPath(String path) throws Exception
    {
        final String    localPath = client.fixForNamespace(path);

        TimeTrace       trace = client.getZookeeperClient().startTracer("GetDataBuilderImpl-Foreground");
        byte[]          responseData = RetryLoop.callWithRetry
        (
            client.getZookeeperClient(),
            new Callable<byte[]>()
            {
                @Override
                public byte[] call() throws Exception
                {
                    return client.getZooKeeper().getData(localPath, false, responseStat);
                }
            }
        );
        trace.commit();

        return decompress ? client.getCompressionProvider().decompress(path, responseData) : responseData;
    }
}
