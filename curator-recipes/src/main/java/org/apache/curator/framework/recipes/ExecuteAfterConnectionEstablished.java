package org.apache.curator.framework.recipes;

import java.util.concurrent.ExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow execution of logic once a ZooKeeper connection becomes available.
 *
 */
public class ExecuteAfterConnectionEstablished
{
	private final static Logger log = LoggerFactory.getLogger(ExecuteAfterConnectionEstablished.class);
	
	/**
	 * Spawns a new new background thread that will block until a connection is available and
	 * then execute the 'runAfterConnection' logic
	 * @param name The name of the spawned thread
	 * @param client The curator client
	 * @param runAfterConnection The logic to run
	 */
	public static void executeAfterConnectionEstablishedInBackground(String name,
																     final CuratorFramework client,
																     final Runnable runAfterConnection)
	{
        //Block until connected
        final ExecutorService executor = ThreadUtils.newSingleThreadExecutor(name);
        executor.submit(new Runnable()
        {
			
			@Override
			public void run()
			{
				try
				{
		        	client.blockUntilConnected();			        
			        runAfterConnection.run();
				}
				catch(Exception e)
				{
					log.error("An error occurred blocking until a connection is available", e);
				}
				finally
				{
					executor.shutdown();
				}
			}
        });
	}
}
