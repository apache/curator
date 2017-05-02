package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;

@FunctionalInterface
public interface TypedModelSpec2<M, P1, P2>
{
    ModelSpec<M> resolved(P1 p1, P2 p2);

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved(Object, Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1, P2> TypedModelSpec2<M, P1, P2> from(ModelSpecBuilder<M> builder, TypedZPath2<P1, P2> path)
    {
        return (p1, p2) -> builder.withPath(path.resolved(p1, p2)).build();
    }
}
