package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;

@FunctionalInterface
public interface TypedModelSpec<M, P1>
{
    /**
     * Resolve into a ZPath using the given parameter
     *
     * @param p1 the parameter
     * @return ZPath
     */
    ModelSpec<M> resolved(P1 p1);

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved(Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1> TypedModelSpec<M, P1> from(ModelSpecBuilder<M> builder, TypedZPath<P1> path)
    {
        return p1 -> builder.withPath(path.resolved(p1)).build();
    }
}
