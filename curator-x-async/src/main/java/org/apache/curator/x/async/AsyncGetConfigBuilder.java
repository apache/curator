package org.apache.curator.x.async;

import org.apache.zookeeper.data.Stat;

public interface AsyncGetConfigBuilder extends AsyncEnsemblable<AsyncStage<byte[]>>
{
    AsyncEnsemblable<AsyncStage<byte[]>> storingStatIn(Stat stat);
}
