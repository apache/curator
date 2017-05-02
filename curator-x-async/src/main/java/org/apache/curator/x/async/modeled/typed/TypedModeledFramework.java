package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ModeledFrameworkBuilder;

@FunctionalInterface
public interface TypedModeledFramework<M, P1>
{
    /**
     * Resolve into a ModeledFramework using the given parameter
     *
     * @param client the curator instance to use
     * @param p1 the parameter
     * @return ZPath
     */
    ModeledFramework<M> resolved(CuratorFramework client, P1 p1);

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder and typed model spec.
     * When {@link #resolved(CuratorFramework, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpec TypedModelSpec
     * @return new TypedModeledFramework
     */
    static <M, P1> TypedModeledFramework<M, P1> from(ModeledFrameworkBuilder<M> frameworkBuilder, TypedModelSpec<M, P1> modelSpec)
    {
        return (client, p1) -> frameworkBuilder.withClient(client).withModelSpec(modelSpec.resolved(p1)).build();
    }
}
