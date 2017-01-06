package org.apache.curator.x.async;

import org.apache.zookeeper.data.Stat;

public interface AsyncGetDataBuilder extends
    AsyncPathable<AsyncStage<byte[]>>
{
    /**
     * Cause the data to be de-compressed using the configured compression provider
     *
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> decompressed();

    /**
     * Have the operation fill the provided stat object
     *
     * @param stat the stat to have filled in
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> storingStatIn(Stat stat);

    /**
     * Have the operation fill the provided stat object
     *
     * @param stat the stat to have filled in
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> decompressedStoringStatIn(Stat stat);
}
