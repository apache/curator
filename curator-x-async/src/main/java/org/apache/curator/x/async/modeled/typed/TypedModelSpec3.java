package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;

public interface TypedModelSpec3<M, P1, P2, P3>
{
    ModelSpec<M> resolved(P1 p1, P2 p2, P3 p3);

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved(Object, Object, Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1, P2, P3> TypedModelSpec3<M, P1, P2, P3> from(ModelSpecBuilder<M> builder, TypedZPath3<P1, P2, P3> path)
    {
        return (p1, p2, p3) -> builder.withPath(path.resolved(p1, p2, p3)).build();
    }
}
