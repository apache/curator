package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ModeledFrameworkBuilder;

@FunctionalInterface
public interface TypedModeledFramework2<M, P1, P2>
{
    ModeledFramework<M> resolved(AsyncCuratorFramework client, P1 p1, P2 p2);

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder and typed model spec.
     * When {@link #resolved(AsyncCuratorFramework, Object, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpec TypedModelSpec
     * @return new TypedModeledFramework
     */
    static <M, P1, P2> TypedModeledFramework2<M, P1, P2> from(ModeledFrameworkBuilder<M> frameworkBuilder, TypedModelSpec2<M, P1, P2> modelSpec)
    {
        return (client, p1, p2) -> frameworkBuilder.withClient(client).withModelSpec(modelSpec.resolved(p1, p2)).build();
    }
}
