package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.recipes.watch.CacheAction;
import org.apache.curator.framework.recipes.watch.CacheSelector;
import java.util.Objects;

/**
 * Utility to bridge an old TreeCacheSelector to a new CuratorCache selector
 */
public class SelectorBridge implements CacheSelector
{
    private final TreeCacheSelector selector;
    private final CacheAction action;

    /**
     * Builder style constructor
     *
     * @param selector the old TreeCacheSelector to bridge
     * @return bridged selector
     */
    public static SelectorBridge wrap(TreeCacheSelector selector)
    {
        return new SelectorBridge(selector);
    }

    /**
     * @param selector the old TreeCacheSelector to bridge
     */
    public SelectorBridge(TreeCacheSelector selector)
    {
        this(selector, CacheAction.STAT_AND_DATA);
    }

    /**
     * @param selector the old TreeCacheSelector to bridge
     * @param action value to return for active paths
     */
    public SelectorBridge(TreeCacheSelector selector, CacheAction action)
    {
        this.selector = Objects.requireNonNull(selector, "selector cannot be null");
        this.action = Objects.requireNonNull(action, "action cannot be null");
    }

    @Override
    public boolean traverseChildren(String basePath, String fullPath)
    {
        return selector.traverseChildren(fullPath);
    }

    @Override
    public CacheAction actionForPath(String basePath, String fullPath)
    {
        return selector.acceptChild(fullPath) ? action : CacheAction.NOT_STORED;
    }
}
