/*
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

package org.apache.curator.x.async.modeled;

import java.util.Arrays;
import java.util.List;

public interface Resolvable {
    /**
     * When creating paths, any node in the path can be set to {@link ZPath#parameter()}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    default Object resolved(Object... parameters) {
        return resolved(Arrays.asList(parameters));
    }

    /**
     * When creating paths, any node in the path can be set to {@link ZPath#parameter()}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    Object resolved(List<Object> parameters);
}
