package org.apache.curator.framework.imps;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.Test;

public class TestConnectionState extends BaseClassForTests
{
    public void beforeSuite(ITestContext context) {
        
    }
    
    @Test
    public void testLostConnectionState() throws Exception
    {
        server.close();
        
        int sessionTimeout = 30000;
        int numRetries = 3;
        int msBetweenRetries = 10000;
        int connectionTimeoutSeconds = 10000;
        
        TestingCluster cluster = new TestingCluster(3);
        CuratorFramework client = null;
        
        try {
            cluster.start();
                   
            client = CuratorFrameworkFactory
                    .builder()
                    .connectString(cluster.getConnectString())
                    .sessionTimeoutMs(sessionTimeout)
                    .connectionTimeoutMs(connectionTimeoutSeconds)
                    .retryPolicy(new RetryNTimes(numRetries, msBetweenRetries))
                    .canBeReadOnly(true).build();
    
            final CountDownLatch lostLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(
                    new ConnectionStateListener()
                    {
                        
                        @Override
                        public void stateChanged(CuratorFramework client, ConnectionState newState)
                        {
                            if(newState == ConnectionState.LOST) {
                                lostLatch.countDown();
                            }
                        }
                    });
    
            client.start();
            
            Assert.assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS), "Failed to establish connection");
            
            //Restart the server        
            cluster.restart();
                        
            //Wait for reconnection
            Assert.assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS), "Failed to reestablish connection");
            
            cluster.stop();
                        
            long stopTime = System.currentTimeMillis();

            long maxWaitTimeMS = ((numRetries + 2) * (msBetweenRetries + connectionTimeoutSeconds));
            boolean lost = lostLatch.await(maxWaitTimeMS, TimeUnit.MILLISECONDS);
            long lostTime = System.currentTimeMillis();
            Assert.assertTrue(lost, "LOST state was not achieved");
            
            //Make sure that the lost event occurred after the right amount of time
            Assert.assertTrue(lostTime - stopTime > numRetries * msBetweenRetries, "LOST state was achieved too early");
            
            
        } finally {       
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }
}
