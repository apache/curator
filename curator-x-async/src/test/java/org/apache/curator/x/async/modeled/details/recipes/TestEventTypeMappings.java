package org.apache.curator.x.async.modeled.details.recipes;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.testng.annotations.Test;

public class TestEventTypeMappings
{
    @Test
    public void testPathChildrenCacheTypes()
    {
        for ( PathChildrenCacheEvent.Type type : PathChildrenCacheEvent.Type.values() )
        {
            ModeledPathChildrenCacheImpl.toType(type);  // throws an exception on unknown types
        }
    }

    @Test
    public void testTreeCacheTypes()
    {
        for ( TreeCacheEvent.Type type : TreeCacheEvent.Type.values() )
        {
            ModeledTreeCacheImpl.toType(type);  // throws an exception on unknown types
        }
    }
}
