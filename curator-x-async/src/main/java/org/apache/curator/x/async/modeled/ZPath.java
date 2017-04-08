package org.apache.curator.x.async.modeled;

import org.apache.curator.x.async.modeled.details.ZPathImpl;

public interface ZPath
{
    ZPath root = ZPathImpl.root;

    static ZPath parse(String fullPath)
    {
        return ZPathImpl.parse(fullPath);
    }

    static ZPath from(String... names)
    {
        ZPath path = root;
        for ( String n : names )
        {
            path = path.at(n);
        }
        return path;
    }

    ZPath at(String child);

    ZPath parent();

    boolean isRoot();

    String fullPath();

    String parentPath();

    String nodeName();
}
