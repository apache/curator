package org.apache.curator.framework.recipes.leader.testing;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyLeaderLatch implements LeaderLatchListener{

	private static final RetryPolicy 	RETRY_POLICY = new ExponentialBackoffRetry(100, 3);
	private static final String 	   	LATCH_PATH   = "/leader/dummy/leaderLatch";
	private static final Logger 	  	LOGGER 		 = LoggerFactory.getLogger(DummyLeaderLatch.class);

	private final LeaderLatch leaderLatch;
	private final CuratorFramework curatorFramework;
	private final AtomicBoolean leader = new AtomicBoolean(false);
	private final LinkedList<Boolean> history = new LinkedList<>();

	public DummyLeaderLatch(final String connectionString, final Timing timing, final String instanceId){

		curatorFramework = CuratorFrameworkFactory.builder()
				.retryPolicy(RETRY_POLICY)
				.connectString(connectionString)
				.sessionTimeoutMs(timing.session())
				.connectionTimeoutMs(timing.connection())
				.build();
		leaderLatch = new LeaderLatch(curatorFramework, LATCH_PATH, instanceId);
		leaderLatch.addListener(this);
	}

	public void start() throws Exception {
		curatorFramework.start();
		curatorFramework.blockUntilConnected();
		createPath(LATCH_PATH);
		leaderLatch.start();
	}

	/**
	 * Starts the service and waits until the latch already has a leader that may be a different node.
	 */
	public void startAndAwaitElection() throws Exception {
		start();
		existsLeader();
	}

	public void awaitElection() throws Exception {
		while (!existsLeader()) {
			Thread.sleep(100L);
		}
	}

	public void awaitLeadershipLost() throws Exception {
		while (existsLeader()) {
			Thread.sleep(100L);
		}
	}

	private boolean existsLeader() throws Exception {
		for (Participant participant : leaderLatch.getParticipants()) {
			if (participant.isLeader()) {
				return true;
			}
		}
		return false;
	}


	public void stop() throws IOException {
		leaderLatch.close();
		curatorFramework.close();
	}

	@Override
	public void isLeader() {
		synchronized (this){
			leader.set(true);
			history.add(true);
		}
	}

	@Override
	public void notLeader() {
		synchronized (this){
			leader.set(false);
			history.add(false);
		}
	}

	public boolean isLeaderAccordingToEvents(){
		return leader.get();
	}

	public boolean isLeaderAccordingToLatch(){
		return leaderLatch.hasLeadership();
	}

	public List<Boolean> getEventHistory(){
		return history;
	}

	/**
	 * Synchronously creates all nodes in a path in ZooKeeper if they don't exist.
	 * <p>
	 * All API calls provided by Curator for path creation with parents do async
	 *
	 * @param path the path
	 */
	private void createPath(final String path) throws Exception {
		final String[] split = path.split("/");
		final StringBuilder stringBuilder = new StringBuilder("/");
		for (final String s : split) {
			if (!s.isEmpty()) {
				stringBuilder.append(s);
				try {
					final String partial = stringBuilder.toString();
					curatorFramework.create().forPath(partial);
				} catch (final KeeperException.NodeExistsException ex) {
					LOGGER.debug("Node " + stringBuilder.toString() + " already existed " + ex);
				}
				stringBuilder.append('/');
			}
		}
	}
}