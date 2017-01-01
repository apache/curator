package org.apache.curator.framework.recipes.watch;

import com.google.common.collect.Iterables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestTreeCacheRandomTree extends BaseTestTreeCache
{
    /**
     * A randomly generated source-of-truth node for {@link #testGiantRandomDeepTree()}
     */
    private static final class TestNode
    {
        String fullPath;
        byte[] data;
        Map<String, TestNode> children = new HashMap<String, TestNode>();

        TestNode(String fullPath, byte[] data)
        {
            this.fullPath = fullPath;
            this.data = data;
        }
    }

    // These constants will produce a tree about 10 levels deep.
    private static final int ITERATIONS = 1000;
    private static final double DIVE_CHANCE = 0.9;
    private static final int TEST_DEPTH = 5;

    private final Random random = new Random();
    private boolean withDepth = false;

    @Test
    public void testGiantRandomDeepTree() throws Exception {
        doTestGiantRandomDeepTree();
    }

    @Test
    public void testGiantRandomDeepTreeWithDepth() throws Exception {
        withDepth = true;
        doTestGiantRandomDeepTree();
    }

    @Test
    public void temp() throws Exception
    {
        String[] strs = {"C/",
            "D/8050ee1037ad1124",
            "C/",
            "U/58add61a4aec0f95",
            "U/58add61a4aec0f95",
            "D/58add61a4aec0f95",
            "C/",
            "D/57d822ba79849e8b",
            "U/",
            "C/",
            "C/86db5aeba8b33121",
            "C/86db5aeba8b33121/fa45819eff99bf8f",
            "C/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581",
            "U/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50",
            "C/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50",
            "U/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0",
            "U/",
            "C/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0",
            "U/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0/f1672fc970f3d64e",
            "U/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0/f1672fc970f3d64e",
            "D/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0/f1672fc970f3d64e",
            "U/86db5aeba8b33121/fa45819eff99bf8f/1a6025e155810581/695419ff443d5e50/be79114eaa5c78a0"
        };

        client.create().forPath("/tree", null);
        CuratorFramework cl = client.usingNamespace("tree");
        for ( String s : strs )
        {
            String opcode = s.substring(0, 1);
            String path = s.substring(1);
            if ( opcode.equals("C") )
            {
                cl.create().forPath(path);
            }
            else if ( opcode.equals("U") )
            {
                byte[] newData = new byte[10];
                random.nextBytes(newData);
                cl.setData().forPath(path, newData);
            }
            else if ( opcode.equals("D") )
            {
                cl.delete().forPath(path);
            }
            else
            {
                throw new RuntimeException("bad");
            }
        }
    }

    /**
     * Randomly construct a large tree of test data in memory, mirror it into ZK, and then use
     * a TreeCache to follow the changes.  At each step, assert that TreeCache matches our
     * source-of-truth test data, and that we see exactly the set of events we expect to see.
     */
    private void doTestGiantRandomDeepTree() throws Exception
    {
        client.create().forPath("/tree", null);
        CuratorFramework cl = client.usingNamespace("tree");
        if ( withDepth )
        {
            cache = buildWithListeners(CuratorCacheBuilder.builder(cl, "/").withMaxDepth(TEST_DEPTH));
        }
        else
        {
            cache = newTreeCacheWithListeners(cl, "/");
        }
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        TestNode root = new TestNode("/", new byte[0]);
        int maxDepth = 0;
        int adds = 0;
        int removals = 0;
        int updates = 0;

        for ( int i = 0; i < ITERATIONS; ++i )
        {
            // Select a node to update, randomly navigate down through the tree
            int depth = 0;
            TestNode last = null;
            TestNode node = root;
            while ( !node.children.isEmpty() && random.nextDouble() < DIVE_CHANCE )
            {
                // Go down a level in the tree.  Select a random child for the next iteration.
                last = node;
                node = Iterables.get(node.children.values(), random.nextInt(node.children.size()));
                ++depth;
            }
            maxDepth = Math.max(depth, maxDepth);

            // Okay we found a node, let's do something interesting with it.
            switch ( random.nextInt(3) )
            {
            case 0:
                // Try a removal if we have no children and we're not the root node.
                if ( node != root && node.children.isEmpty() )
                {
                    // Delete myself from parent.
                    TestNode removed = last.children.remove(ZKPaths.getNodeFromPath(node.fullPath));
                    Assert.assertSame(node, removed);

                    // Delete from ZK
                    cl.delete().forPath(node.fullPath);

                    // TreeCache should see the delete.
                    if (shouldSeeEventAt(node.fullPath))
                    {
                        assertEvent(CacheEvent.NODE_DELETED, node.fullPath);
                    }
                    ++removals;
                }
                break;
            case 1:
                // Do an update.
                byte[] newData = new byte[10];
                random.nextBytes(newData);

                if ( Arrays.equals(node.data, newData) )
                {
                    // Randomly generated the same data! Very small chance, just skip.
                    continue;
                }

                // Update source-of-truth.
                node.data = newData;

                // Update in ZK.
                cl.setData().forPath(node.fullPath, node.data);

                // TreeCache should see the update.
                if (shouldSeeEventAt(node.fullPath))
                {
                    assertEvent(CacheEvent.NODE_CHANGED, node.fullPath, node.data);
                }

                ++updates;
                break;
            case 2:
                // Add a new child.
                String name = Long.toHexString(random.nextLong());
                if ( node.children.containsKey(name) )
                {
                    // Randomly generated the same name! Very small chance, just skip.
                    continue;
                }

                // Add a new child to our test tree.
                byte[] data = new byte[10];
                random.nextBytes(data);
                TestNode child = new TestNode(ZKPaths.makePath(node.fullPath, name), data);
                node.children.put(name, child);

                // Add to ZK.
                cl.create().forPath(child.fullPath, child.data);

                // TreeCache should see the add.
                if (shouldSeeEventAt(child.fullPath))
                {
                    assertEvent(CacheEvent.NODE_CREATED, child.fullPath, child.data);
                }

                ++adds;
                break;
            }

            // Each iteration, ensure the cached state matches our source-of-truth tree.
            assertNodeEquals(cache.get("/"), root);
            assertTreeEquals(cache, root, 0);
        }

        // Typical stats for this test: maxDepth: 10, adds: 349, removals: 198, updates: 320
        // We get more adds than removals because removals only happen if we're at a leaf.
        System.out.println(String.format("maxDepth: %s, adds: %s, removals: %s, updates: %s", maxDepth, adds, removals, updates));
        assertNoMoreEvents();
    }

    /**
     * Returns true we should see an event at this path based on maxDepth, false otherwise.
     */
    private boolean shouldSeeEventAt(String fullPath)
    {
        return !withDepth || ZKPaths.split(fullPath).size() <= TEST_DEPTH;
    }

    /**
     * Recursively assert that current children equal expected children.
     */
    private void assertTreeEquals(CuratorCache cache, TestNode expectedNode, int depth)
    {
        String path = expectedNode.fullPath;
        Map<String, CachedNode> cacheChildren = cache.childrenAtPath(path);
        Assert.assertNotNull(cacheChildren, path);

        if (withDepth && depth == TEST_DEPTH) {
            return;
        }

        Assert.assertEquals(cacheChildren.keySet(), expectedNode.children.keySet(), path);

        for ( Map.Entry<String, TestNode> entry : expectedNode.children.entrySet() )
        {
            String nodeName = entry.getKey();
            CachedNode childData = cacheChildren.get(nodeName);
            TestNode expectedChild = entry.getValue();
            assertNodeEquals(childData, expectedChild);
            assertTreeEquals(cache, expectedChild, depth + 1);
        }
    }

    /**
     * Assert that the given node data matches expected test node data.
     */
    private static void assertNodeEquals(CachedNode actualChild, TestNode expectedNode)
    {
        String path = expectedNode.fullPath;
        Assert.assertNotNull(actualChild, path);
        Assert.assertEquals(actualChild.getData(), expectedNode.data, path);
    }
}
