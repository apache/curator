package org.apache.curator.x.async.modeled;

import java.util.Arrays;
import java.util.List;

public interface Resolvable
{
    /**
     * When creating paths, any node in the path can be set to {@link org.apache.curator.x.async.modeled.ZPath#parameterNodeName}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    default Object resolved(Object... parameters)
    {
        return resolved(Arrays.asList(parameters));
    }

    /**
     * When creating paths, any node in the path can be set to {@link org.apache.curator.x.async.modeled.ZPath#parameterNodeName}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    Object resolved(List<Object> parameters);
}
