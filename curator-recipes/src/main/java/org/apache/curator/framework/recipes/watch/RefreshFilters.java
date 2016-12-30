package org.apache.curator.framework.recipes.watch;

public class RefreshFilters
{
    private static final RefreshFilter singleLevel = new RefreshFilter()
    {
        @Override
        public boolean descend(String mainPath, String checkPath)
        {
            return mainPath.equals(checkPath);
        }
    };

    private static final RefreshFilter tree = new RefreshFilter()
    {
        @Override
        public boolean descend(String mainPath, String checkPath)
        {
            return true;
        }
    };

    public static RefreshFilter singleLevel()
    {
        return singleLevel;
    }

    public static RefreshFilter tree()
    {
        return tree;
    }

    private RefreshFilters()
    {
    }
}
