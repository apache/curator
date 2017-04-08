package org.apache.curator.x.async.modeled.details;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.common.PathUtils;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class ZPathImpl implements ZPath
{
    public static final ZPath root = new ZPathImpl(Collections.singletonList(ZKPaths.PATH_SEPARATOR));

    private final List<String> nodes;

    public static ZPath parse(String fullPath)
    {
        PathUtils.validatePath(fullPath);
        List<String> nodes = ImmutableList.<String>builder()
            .add(ZKPaths.PATH_SEPARATOR)
            .addAll(Splitter.on(ZKPaths.PATH_SEPARATOR).omitEmptyStrings().splitToList(fullPath))
            .build();
        return new ZPathImpl(nodes);
    }

    @Override
    public ZPath at(String child)
    {
        return new ZPathImpl(nodes, child);
    }

    @Override
    public ZPath parent()
    {
        checkRootAccess();
        return new ZPathImpl(nodes.subList(0, nodes.size() - 1));
    }

    @Override
    public boolean isRoot()
    {
        return nodes.size() == 1;
    }

    @Override
    public String fullPath()
    {
        return buildFullPath(nodes.size());
    }

    @Override
    public String parentPath()
    {
        checkRootAccess();
        return buildFullPath(nodes.size() - 1);
    }

    @Override
    public String nodeName()
    {
        return nodes.get(nodes.size() - 1);
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ZPathImpl zPaths = (ZPathImpl)o;

        return nodes.equals(zPaths.nodes);
    }

    @Override
    public int hashCode()
    {
        return nodes.hashCode();
    }

    @Override
    public String toString()
    {
        return "ZPathImpl{" + "nodes=" + nodes + '}';
    }

    private ZPathImpl(List<String> nodes)
    {
        this.nodes = Objects.requireNonNull(nodes, "nodes cannot be null");
    }

    private ZPathImpl(List<String> nodes, String child)
    {
        PathUtils.validatePath(ZKPaths.PATH_SEPARATOR + child);
        this.nodes = ImmutableList.<String>builder()
            .addAll(nodes)
            .add(child)
            .build();
    }

    private void checkRootAccess()
    {
        if ( isRoot() )
        {
            throw new NoSuchElementException("The root has no parent");
        }
    }

    private String buildFullPath(int size)
    {
        boolean addSeparator = false;
        StringBuilder str = new StringBuilder();
        for ( int i = 0; i < size; ++i )
        {
            if ( i > 1 )
            {
                str.append(ZKPaths.PATH_SEPARATOR);
            }
            str.append(nodes.get(i));
        }
        return str.toString();
    }
}
