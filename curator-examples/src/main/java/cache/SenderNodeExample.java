package cache;

import com.google.common.base.Objects;
import com.sun.xml.internal.bind.v2.runtime.RuntimeUtil;
import framework.CreateClientExamples;
import framework.CrudExamples;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

/**
 * Created by yuexiaojun on 16/4/6.
 */
public class SenderNodeExample {


    public static void main(String[] args) {

        String namespace = "ialock_inc";
        String nodePath = "/node";
        String instructionPath = "/instruction";
        String connectionString = "127.0.0.1:2181";

        String instructionId = "12345";

        String senderId = "192.168.1.1";

        String senderPath = nodePath + "/" + senderId;

        String receiverId = "192.168.1.2";


        System.out.println("zk start...");
        CuratorFramework client = CreateClientExamples.createSimple(connectionString);
        client.start();
        client.usingNamespace(namespace);

        //初始化机器对应的zk节点
        try {
            CrudExamples.create(client, nodePath, String.valueOf(System.currentTimeMillis()).getBytes());
            CrudExamples.create(client, senderPath, String.valueOf(System.currentTimeMillis()).getBytes());
            PathChildrenCache cache = new PathChildrenCache(client, senderPath, true);
            cache.start();
            addListener(cache);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            CrudExamples.create(client, instructionPath, String.valueOf(System.currentTimeMillis()).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            CrudExamples.create(client, senderPath + "/" + instructionId, String.valueOf(System.currentTimeMillis()).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            CrudExamples.create(client, instructionPath + "/" + instructionId, senderId.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("zk start ok...");

        try {
            Thread.sleep(1000000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


    private static void addListener(PathChildrenCache cache) {
        // a PathChildrenCacheListener is optional. Here, it's used just to log changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println(event.toString());
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_UPDATED: {
                        System.out.println(event.toString());
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));

                        try {
                            long oldTime = Long.valueOf(new String(event.getData().getData()));
                            System.out.println("事件通知延迟  " + (System.currentTimeMillis() - oldTime) + " ms");
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        break;
                    }

                    case CHILD_REMOVED: {
                        System.out.println(event.toString());
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }


}
