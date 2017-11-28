package org.apache.curator.framework.recipes.leader;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.IOException;

import org.apache.curator.framework.recipes.leader.testing.DummyLeaderLatch;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/** Tests issue: CURATOR-444: Zookeeper node is isolated from other zookeepers but not from curator
 *  temporally */


public class TestLeaderLatchIsolatedZookeeper {

	private TestingCluster cluster = new TestingCluster(3);
	private Timing timing = new Timing();
	private DummyLeaderLatch firstClient  = new DummyLeaderLatch(cluster.getConnectString(), timing, "First");
	private DummyLeaderLatch secondClient = new DummyLeaderLatch(cluster.getConnectString(), timing, "Second");

	@BeforeTest
	public void beforeTest() throws Exception {
		cluster.start();
		firstClient.startAndAwaitElection();
		secondClient.startAndAwaitElection();
	}

	@AfterTest
	public void afterTest() throws IOException {
		firstClient.stop();
		secondClient.stop();
		cluster.close();
	}

	@Test
	public void testThatStartsWithOnlyOneLeader() throws Exception {
		assertNotEquals(firstClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToLatch());
	}

	@Test
	public void testThatStartCoherent() throws Exception {
		assertEquals(firstClient.isLeaderAccordingToLatch(),  firstClient.isLeaderAccordingToEvents());
		assertEquals(secondClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToEvents());
	}

	@Test
	public void testThatResistsNetworkGlitches() throws Exception {

		for(TestingZooKeeperServer server : cluster.getServers()){

			// change to real glitch
			if( server.getQuorumPeer().getPeerState().equals(QuorumPeer.ServerState.LEADING)){

				server.getQuorumPeer().setPeerState(QuorumPeer.ServerState.LOOKING);

				System.out.println("CHANGED SERVER" + server);
				firstClient.awaitElection();

				assertEquals(firstClient.isLeaderAccordingToLatch(),  firstClient.isLeaderAccordingToEvents());
				assertEquals(secondClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToEvents());
				assertNotEquals(firstClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToLatch());

			}
		}
	}
}
