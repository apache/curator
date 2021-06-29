package org.apache.curator.x.discovery.details;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DiscoveryPathConstructorImplTest
{

    @Test
    public void testCanonicalDiscoveryPathConstructor()
    {
        Assert.expectThrows(IllegalArgumentException.class, new Assert.ThrowingRunnable()
            {
                @Override
                public void run() throws Throwable {
                    new DiscoveryPathConstructorImpl(null);
                }
            }
        );
    }

    @Test
    public void testGetBasePath() throws Exception
    {
        Assert.assertEquals(new DiscoveryPathConstructorImpl("/foo/bar").getBasePath(), "/foo/bar");
        Assert.assertEquals(new DiscoveryPathConstructorImpl("foo/bar").getBasePath(), "/foo/bar");
        Assert.assertEquals(new DiscoveryPathConstructorImpl("foo/bar/").getBasePath(), "/foo/bar");
        Assert.assertEquals(new DiscoveryPathConstructorImpl("/").getBasePath(), "/");
        Assert.assertEquals(new DiscoveryPathConstructorImpl("").getBasePath(), "/");
    }

    @Test
    public void testGetPathForInstances() throws Exception
    {
        DiscoveryPathConstructorImpl constructor = new DiscoveryPathConstructorImpl("/foo/bar");
        Assert.assertEquals(constructor.getPathForInstances("baz"), "/foo/bar/baz");
        Assert.assertEquals(constructor.getPathForInstances(""), "/foo/bar");
        Assert.assertEquals(constructor.getPathForInstances(null), "/foo/bar");

        constructor = new DiscoveryPathConstructorImpl("foo/bar");
        Assert.assertEquals(constructor.getPathForInstances("baz"), "/foo/bar/baz");
        Assert.assertEquals(constructor.getPathForInstances(""), "/foo/bar");
        Assert.assertEquals(constructor.getPathForInstances(null), "/foo/bar");
    }

    @Test
    public void testGetPathForInstance() throws Exception
    {
        DiscoveryPathConstructorImpl constructor = new DiscoveryPathConstructorImpl("/foo");
        Assert.assertEquals(constructor.getPathForInstance("bar", "baz"), "/foo/bar/baz");
        Assert.assertEquals(constructor.getPathForInstance("", "baz"), "/foo/baz");
        Assert.assertEquals(constructor.getPathForInstance(null, "baz"), "/foo/baz");
        Assert.assertEquals(constructor.getPathForInstance("bar", ""), "/foo/bar");
        Assert.assertEquals(constructor.getPathForInstance("bar", null), "/foo/bar");
        Assert.assertEquals(constructor.getPathForInstance("", ""), "/foo");
        Assert.assertEquals(constructor.getPathForInstance(null, null), "/foo");
    }

}