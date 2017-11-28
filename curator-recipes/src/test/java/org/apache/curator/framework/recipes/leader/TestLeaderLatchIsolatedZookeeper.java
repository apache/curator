/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.framework.recipes.leader;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.apache.curator.framework.recipes.leader.testing.DummyLeaderLatch;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests issue: CURATOR-444: Zookeeper node is isolated from other zookeepers but not from curator
 *  temporally */


public class TestLeaderLatchIsolatedZookeeper {

	private static final Logger logger = LoggerFactory.getLogger(TestLeaderLatchIsolatedZookeeper.class);
	private static final int CONNECTION_TIMEOUT_MS = 15000;
	private static final int SESSION_TIMEOUT_MS = 10000;

	private TestingCluster cluster;
	private DummyLeaderLatch firstClient;
	private DummyLeaderLatch secondClient;

	@BeforeMethod
	public void beforeMethod() throws Exception {
		cluster = new TestingCluster(3);
		cluster.start();
		firstClient  = new DummyLeaderLatch(cluster.getConnectString(), SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, "First");
		secondClient = new DummyLeaderLatch(cluster.getConnectString(), SESSION_TIMEOUT_MS, CONNECTION_TIMEOUT_MS, "Second");
		firstClient.startAndAwaitElection();
		secondClient.startAndAwaitElection();
		logger.info("Session: " + SESSION_TIMEOUT_MS + " connection " + CONNECTION_TIMEOUT_MS );
	}

	@AfterMethod
	public void afterMethod() throws IOException {
		DummyLeaderLatch.resetHistory();
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

	@Test(invocationCount = 10)
	public void testThatResistsNetworkGlitches() throws Exception {

		blockLeaderListeningForSomeTime(SESSION_TIMEOUT_MS);

		Thread.sleep(SESSION_TIMEOUT_MS * 3);

		assertTrue(isHistoryValid(), "History is not valid: " + DummyLeaderLatch.getEventHistory());

		assertEquals(   firstClient.isLeaderAccordingToLatch(),  firstClient.isLeaderAccordingToEvents());
		assertEquals(  secondClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToEvents());
		assertNotEquals(firstClient.isLeaderAccordingToLatch(), secondClient.isLeaderAccordingToLatch());
	}

	private void blockLeaderListeningForSomeTime(long milliseconds) throws InterruptedException {
		for(TestingZooKeeperServer server : cluster.getServers()) {

			if (server.getQuorumPeer().leader != null) {
				Leader leader = server.getQuorumPeer().leader;

				for (LearnerHandler learnerHandler : leader.getLearners()) {

					logger.info("Locking " + learnerHandler.getName());
					HandlerLocker locker = new HandlerLocker(learnerHandler, milliseconds);
					locker.start();
				}
			}
		}
	}

	private static class HandlerLocker extends Thread{

		private final long milliseconds;
		private final LearnerHandler learnerHandler;

		public HandlerLocker(LearnerHandler learnerHandler, long milliseconds){
			this.learnerHandler = learnerHandler;
			this.milliseconds = milliseconds;
		}

		@Override
		public void run() {
			//suspendThread();
			//closeSocket();
			//lockSocket();
			forceTimeout();
		}

		private void forceTimeout(){
			try{
				Socket socket = learnerHandler.getSocket();
				int oldTimeout = learnerHandler.getSocket().getSoTimeout();
				socket.setSoTimeout(1);
				Thread.sleep(SESSION_TIMEOUT_MS);
				socket.setSoTimeout(oldTimeout);

			}catch (SocketException ex){
				logger.error("Error forcing timeout", ex);
			} catch (InterruptedException ex) {
				logger.error("Interrupted while forcing timeout", ex);
			}
		}


		private void lockSocket(){
			try{
				logger.info("[" + System.currentTimeMillis() + "] locking socket for learner handler " + learnerHandler.getName() + " for " + milliseconds);

				Socket socket = learnerHandler.getSocket();
				synchronized (socket){
					learnerHandler.getSocket().wait(milliseconds);
				}
				logger.info("[" + System.currentTimeMillis() + "] freeing socket for learner handler " + learnerHandler.getName() + " after " + milliseconds);

			}catch (InterruptedException ex){
				String message = "Failed to lock socket for " + milliseconds + " ms";
				logger.error(message);
				Thread.currentThread().interrupt();
				throw new RuntimeException(message, ex);
			}
		}

		private void suspendThread(){
			try{
				logger.info("[" + System.currentTimeMillis() + "] Suspending learner handler " + learnerHandler.getName() + " for " + milliseconds);

				learnerHandler.suspend();
				Thread.sleep(milliseconds);
				learnerHandler.resume();

				logger.info("[" + System.currentTimeMillis() + "] Resume learner handler " + learnerHandler.getName() + " after " + milliseconds);


			}catch (InterruptedException ex){

				String message = "Failed to wait " + milliseconds + " ms";
				logger.error(message);
				Thread.currentThread().interrupt();
				throw new RuntimeException(message, ex);
			}

		}

		private void closeSocket(){
			try {
				logger.info("Closing socket for learned handler " + learnerHandler.getName());
				learnerHandler.getSocket().close();
				Thread.sleep(milliseconds);
			}catch (Exception ex){
				logger.error("Error closing socket", ex);
			}
		}
	}

	private boolean isHistoryValid(){
		logger.info("EVENTS HISTORY :" + DummyLeaderLatch.getEventHistory());

		boolean hadLeader = false;
		String lastLeaderId = null;
		for( DummyLeaderLatch.History history : DummyLeaderLatch.getEventHistory()){
			if( history.isLeader()){
				if( hadLeader ){
					if(lastLeaderId.equals(history.getId())){
						logger.warn("Redundant ToLeader");
					}else{
						return false;
					}
				}
				hadLeader = true;
				lastLeaderId = history.getId();
			}else{
				if(!lastLeaderId.equals(history.getId())) {
					logger.warn("Redundant NotLeader");
				}
				hadLeader = false;
				lastLeaderId = null;
			}
		}
		return true;
	}
}