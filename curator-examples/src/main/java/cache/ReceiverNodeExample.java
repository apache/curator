package cache;

import framework.CreateClientExamples;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.Charset;

/**
 * Created by yuexiaojun on 16/4/6.
 */
public class ReceiverNodeExample {
    public static void main(String[] args) {

        String namespace="ialock_inc";
        String nodePath="/node";
        String instructionPath="/instruction";
        String connectionString="127.0.0.1:2181";

        String instructionId="12345";

        String senderId="192.168.1.1";

        String senderPath=nodePath+"/"+senderId;

        String receiverId="192.168.1.2";


        System.out.println("zk start...");
        CuratorFramework client= CreateClientExamples.createSimple(connectionString);
        client.start();
        client.usingNamespace(namespace);

        try {
            String instructionData = new String ( client.getData().forPath(instructionPath+"/"+instructionId), Charset.forName("UTF-8"));

            String instructionNodePath=nodePath+"/"+instructionData+"/"+instructionId;

            client.setData().forPath(instructionNodePath,String.valueOf(System.currentTimeMillis()).getBytes());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
