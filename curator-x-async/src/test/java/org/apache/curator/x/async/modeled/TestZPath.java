package org.apache.curator.x.async.modeled;

import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.details.ZPathImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZPath
{
    @Test
    public void testRoot()
    {
        Assert.assertEquals(ZPath.root.nodeName(), ZKPaths.PATH_SEPARATOR);
        Assert.assertEquals(ZPath.root, ZPathImpl.root);
        Assert.assertTrue(ZPath.root.isRoot());
        Assert.assertEquals(ZPath.root.at("foo").parent(), ZPath.root);
        Assert.assertTrue(ZPath.root.at("foo").parent().isRoot());
    }

    @Test
    public void testBasic()
    {
        ZPath path = ZPath.root.at("one").at("two");
        Assert.assertFalse(path.isRoot());
        Assert.assertEquals(path, ZPath.root.at("one").at("two"));
        Assert.assertNotEquals(path, ZPath.root.at("onex").at("two"));
        Assert.assertEquals(path.nodeName(), "two");
        Assert.assertEquals(path.fullPath(), "/one/two");
        Assert.assertEquals(path.parentPath(), "/one");
    }

    @Test
    public void testParsing()
    {
        Assert.assertEquals(ZPath.parse("/"), ZPath.root);
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.root.at("one").at("two").at("three"));
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.from("one", "two", "three"));
    }
}
