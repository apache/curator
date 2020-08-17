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
package org.apache.curator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.Compatibility;
import org.junit.jupiter.api.Tag;

public class TestIs36 extends CuratorTestBase
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    @Tag(zk36Group)
    public void testIsZk36()
    {
        assertTrue(Compatibility.hasGetReachableOrOneMethod());
        assertTrue(Compatibility.hasAddrField());
        assertTrue(Compatibility.hasPersistentWatchers());
    }

    @Override
    protected void createServer()
    {
        // NOP
    }
}
