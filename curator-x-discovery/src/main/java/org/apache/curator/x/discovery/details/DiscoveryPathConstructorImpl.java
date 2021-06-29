package org.apache.curator.x.discovery.details;


import com.google.common.base.Preconditions;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.DiscoveryPathConstructor;

/**
 * A standard constructor, it uses standard path constructing strategy by applying name to basePath
 *
 * Example: basePath = /foo/bar, serviceName = baz, pathToInstances = /foo/bar/baz
 */
public class DiscoveryPathConstructorImpl implements DiscoveryPathConstructor
{

    private final String basePath;

    public DiscoveryPathConstructorImpl(String basePath)
    {
        Preconditions.checkArgument(basePath != null, "basePath cannot be null");
        if ( basePath.endsWith(ZKPaths.PATH_SEPARATOR))
        {
            basePath = basePath.substring(0, basePath.length() - 1);
        }
        if ( !basePath.startsWith(ZKPaths.PATH_SEPARATOR))
        {
            basePath = ZKPaths.PATH_SEPARATOR + basePath;
        }

        this.basePath = basePath;
    }

    @Override
    public String getBasePath()
    {
        return basePath;
    }

    @Override
    public String getPathForInstances(String name)
    {
        return ZKPaths.makePath(basePath, name);
    }

    @Override
    public String getPathForInstance(String name, String id)
    {
        return ZKPaths.makePath(getPathForInstances(name), id);
    }

}
