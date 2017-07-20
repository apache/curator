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
package org.apache.curator.test;

import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Zk35MethodInterceptor implements IMethodInterceptor
{
    public static final String zk35Group = "zk35";

    @Override
    public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context)
    {
        if ( !Compatibility.isZK34() )
        {
            return methods;
        }

        List<IMethodInstance> filteredMethods = new ArrayList<>();
        for ( IMethodInstance method : methods )
        {
            if ( !isInGroup(method.getMethod().getGroups()) )
            {
                filteredMethods.add(method);
            }
        }
        return filteredMethods;
    }

    private boolean isInGroup(String[] groups)
    {
        return (groups != null) && Arrays.asList(groups).contains(zk35Group);
    }
}
