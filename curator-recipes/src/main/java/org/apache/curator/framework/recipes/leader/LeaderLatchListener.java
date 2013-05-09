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

/**
 * A LeaderLatchListener can be used to be notified asynchronously about when the state of the LeaderLatch has changed.
 *
 * Note that just because you are in the middle of one of these method calls, it does not necessarily mean that
 * hasLeadership() is the corresponding true/false value.  It is possible for the state to change behind the scenes
 * before these methods get called.  The contract is that if that happens, you should see another call to the other
 * method pretty quickly.
 */
public interface LeaderLatchListener
{
  /**
   * This is called when the LeaderLatch's state goes from hasLeadership = false to hasLeadership = true.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has fallen back to false.  If
   * this occurs, you can expect {@link #notLeader()} to also be called.
   */
  public void isLeader();

  /**
   * This is called when the LeaderLatch's state goes from hasLeadership = true to hasLeadership = false.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has become true.  If
   * this occurs, you can expect {@link #isLeader()} to also be called.
   */
  public void notLeader();
}
