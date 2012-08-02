package com.netflix.curator.framework.imps;

import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;

class NamespaceImpl
{
    private final CuratorFrameworkImpl client;
    private final String namespace;
    private final EnsurePath ensurePath;

    NamespaceImpl(CuratorFrameworkImpl client, String namespace)
    {
        this.client = client;
        this.namespace = namespace;
        ensurePath = (namespace != null) ? new EnsurePath(ZKPaths.makePath("/", namespace)) : null;
    }

    String getNamespace()
    {
        return namespace;
    }

    String    unfixForNamespace(String path)
    {
        if ( (namespace != null) && (path != null) )
        {
            String      namespacePath = ZKPaths.makePath(namespace, null);
            if ( path.startsWith(namespacePath) )
            {
                path = (path.length() > namespacePath.length()) ? path.substring(namespacePath.length()) : "/";
            }
        }
        return path;
    }

    String    fixForNamespace(String path)
    {
        if ( ensurePath != null )
        {
            try
            {
                ensurePath.ensure(client.getZookeeperClient());
            }
            catch ( Exception e )
            {
                client.logError("Ensure path threw exception", e);
            }
        }

        return ZKPaths.fixForNamespace(namespace, path);
    }

    EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return new EnsurePath(fixForNamespace(path));
    }
}
