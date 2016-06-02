package org.apache.curator.framework.recipes.locks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;

public class TestInterProcessMutexNotReconnecting extends BaseClassForTests
{
    @org.testng.annotations.Test
    public void test() throws Exception
    {
        final String SEMAPHORE_PATH = "/test";
        final int MAX_SEMAPHORES = 1;
        final int NUM_CLIENTS = 10;
        
        server.start();
        
        CuratorFramework client = null;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);
        
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicBoolean run = new AtomicBoolean(true);
        
        try {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), 5000, 5000, new RetryOneTime(1));
            client.start();
            
            final CuratorFramework lClient = client;
            
            for(int i = 0; i < NUM_CLIENTS; ++i)
            {
                executor.execute(new Runnable()
                    {
                    
                    @Override
                    public void run()
                    {
                        while(run.get())
                        {
                            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(lClient, SEMAPHORE_PATH, MAX_SEMAPHORES);
                            System.err.println(Thread.currentThread() + "Acquiring");
                            Lease lease = null;
                            try
                            {
                                lease = semaphore.acquire();
                                System.err.println(Thread.currentThread() + "Acquired");
                                counter.incrementAndGet();
                                Thread.sleep(2000);
                            }
                            catch(InterruptedException e)
                            {
                                System.err.println("Interrupted");
                                Thread.currentThread().interrupt();
                                break;
                            }
                            catch(KeeperException e)
                            {
                                try
                                {
                                    Thread.sleep(2000);
                                }
                                catch(InterruptedException e2)
                                {
                                    System.err.println("Interrupted");
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                            catch(Exception e)
                            {
                                e.printStackTrace();
                            }
                            finally
                            {
                                if(lease != null) {
                                    semaphore.returnLease(lease);
                                }
                            }
                        }
                    }
                    });
            }
            

            final AtomicBoolean lost = new AtomicBoolean(false);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                   System.err.println("New state : " + newState);
                   
                   if(newState == ConnectionState.LOST) {
                       lost.set(true);
                   }
                }
            });
            
            Thread.sleep(2000);
            
            System.err.println("Stopping server");
            server.stop();
            System.err.println("Stopped server");
            
            while(!lost.get())
            {
                Thread.sleep(1000);
            }
            
            int preRestartCount = counter.get();
            
            System.err.println("Restarting server");
            server.restart();
            
            long startCheckTime = System.currentTimeMillis();
            while(true)
            {
                if(counter.get() > preRestartCount)
                {
                    break;
                }
                else if((System.currentTimeMillis() - startCheckTime) > 30000)
                {
                    Assert.fail("Semaphores not reacquired after restart");
                }
            }

        }
        finally
        {
            run.set(false);
            executor.shutdownNow();
            CloseableUtils.closeQuietly(client);
        }
    }
}
