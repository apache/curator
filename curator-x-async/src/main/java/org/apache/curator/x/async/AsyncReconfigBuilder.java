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
package org.apache.curator.x.async;

import org.apache.zookeeper.data.Stat;
import java.util.List;

/**
 * Builder for reconfigs
 */
public interface AsyncReconfigBuilder
{
    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is: <code>server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]</code>
     *
     * @param servers The new server list
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers);

    /**
     * Adds servers to join the ensemble and/or servers to leave the ensemble. The format for <strong>joining</strong>
     * is: <code>server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]</code>. The format
     * for <strong>leaving</strong> is a list of server IDs.
     *
     * @param joining The servers joining
     * @param leaving The servers leaving
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving);

    /**
     * Same as {@link #withNewMembers(java.util.List)} but allows specified the configuration version to use.
     * By default the configuration version is -1.
     *
     * @param servers The new server list
     * @param fromConfig the config version to use
     * @see #withNewMembers(java.util.List)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, long fromConfig);

    /**
     * Specify joiners, leaves and config version. By default the configuration version is -1.
     *
     * @param joining The servers joining
     * @param leaving The servers leaving
     * @param fromConfig the config version to use
     * @see #withJoiningAndLeaving(java.util.List, java.util.List)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, long fromConfig);

    /**
     * Same as {@link #withNewMembers(java.util.List)} but allows a stat to hold the stat info from "/zookeeper/config"
     *
     * @param servers The servers joining.
     * @param stat stat to hold the stat value
     * @see #withNewMembers(java.util.List)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat);

    /**
     * Same as {@link #withJoiningAndLeaving(java.util.List, java.util.List)}
     * but allows a stat to hold the stat info from "/zookeeper/config"
     *
     * @param joining The servers joining
     * @param leaving The servers leaving
     * @param stat stat to hold the stat value
     * @see #withJoiningAndLeaving(java.util.List, java.util.List)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat);

    /**
     * Same as {@link #withNewMembers(java.util.List)} with stat and config version
     *
     * @param servers The servers joining.
     * @param stat stat to hold the stat value
     * @param fromConfig the config version to use
     * @see #withNewMembers(java.util.List, long)
     * @see #withNewMembers(java.util.List, org.apache.zookeeper.data.Stat)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat, long fromConfig);

    /**
     * Same as {@link #withJoiningAndLeaving(java.util.List, java.util.List)} with stat and config version
     *
     * @param joining The servers joining
     * @param leaving The servers leaving
     * @param stat stat to hold the stat value
     * @param fromConfig the config version to use
     * @see #withJoiningAndLeaving(java.util.List, java.util.List, long)
     * @see #withJoiningAndLeaving(java.util.List, java.util.List, org.apache.zookeeper.data.Stat)
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat, long fromConfig);
}
