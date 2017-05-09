package org.apache.curator.x.async.modeled.details;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.data.Stat;

class VersionedModeledFrameworkImpl<T> implements VersionedModeledFramework<T>
{
    private final ModeledFramework<T> client;

    VersionedModeledFrameworkImpl(ModeledFramework<T> client)
    {
        this.client = client;
    }

    @Override
    public AsyncStage<String> set(Versioned<T> model)
    {
        return client.set(model.model(), model.version());
    }

    @Override
    public AsyncStage<String> set(Versioned<T> model, Stat storingStatIn)
    {
        return client.set(model.model(), storingStatIn, model.version());
    }

    @Override
    public AsyncStage<Versioned<T>> read()
    {
        return read(null);
    }

    @Override
    public AsyncStage<Versioned<T>> read(Stat storingStatIn)
    {
        Stat localStat = (storingStatIn != null) ? storingStatIn : new Stat();
        ModelStage<Versioned<T>> modelStage = new ModelStage<>();
        client.read(localStat).whenComplete((model, e) -> {
           if ( e != null )
           {
               modelStage.completeExceptionally(e);
           }
           else
           {
               modelStage.complete(Versioned.from(model, localStat.getVersion()));
           }
        });
        return modelStage;
    }

    @Override
    public AsyncStage<Stat> update(Versioned<T> model)
    {
        return client.update(model.model(), model.version());
    }

    @Override
    public CuratorOp updateOp(Versioned<T> model)
    {
        return client.updateOp(model.model(), model.version());
    }
}
