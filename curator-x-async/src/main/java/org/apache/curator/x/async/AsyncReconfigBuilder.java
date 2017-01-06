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

public interface AsyncReconfigBuilder
{
    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, long fromConfig);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, long fromConfig);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat, long fromConfig);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat, long fromConfig);
}
