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

public enum WatchMode
{
    /**
     * The {@link java.util.concurrent.CompletionStage}&lt;org.apache.zookeeper.WatchedEvent&gt; will only
     * complete on successful trigger. i.e. connection issues are ignored
     */
    successOnly,

    /**
     * The {@link java.util.concurrent.CompletionStage}&lt;org.apache.zookeeper.WatchedEvent&gt; will only
     * completeExceptionally. Successful trigger is ignored. Connection exceptions are
     * of type: {@link org.apache.curator.x.async.AsyncEventException}.
     */
    stateChangeOnly,

    /**
     * The {@link java.util.concurrent.CompletionStage}&lt;org.apache.zookeeper.WatchedEvent&gt; will
     * complete for both successful trigger and connection exceptions. Connection exceptions are
     * of type: {@link org.apache.curator.x.async.AsyncEventException}. Note: this is the default
     * watch mode.
     */
    stateChangeAndSuccess
}
