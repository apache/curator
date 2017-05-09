package org.apache.curator.x.async.modeled.versioned;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

public interface VersionedModeledFramework<T>
{
    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#set(Object)
     */
    AsyncStage<String> set(Versioned<T> model);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#set(Object, org.apache.zookeeper.data.Stat)
     */
    AsyncStage<String> set(Versioned<T> model, Stat storingStatIn);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#read()
     */
    AsyncStage<Versioned<T>> read();

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#read(org.apache.zookeeper.data.Stat)
     */
    AsyncStage<Versioned<T>> read(Stat storingStatIn);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#updateOp(Object)
     */
    AsyncStage<Stat> update(Versioned<T> model);

    /**
     * @see org.apache.curator.x.async.modeled.ModeledFramework#updateOp(Object) 
     */
    CuratorOp updateOp(Versioned<T> model);
}
