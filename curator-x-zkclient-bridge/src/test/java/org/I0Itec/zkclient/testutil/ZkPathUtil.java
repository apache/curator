/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.I0Itec.zkclient.testutil;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;

// JLZ - copied from https://raw.github.com/sgroschupf/zkclient/master/src/main/java/org/I0Itec/zkclient/util/ZkPathUtil.java
public class ZkPathUtil {

    public static String leadingZeros(long number, int numberOfLeadingZeros) {
        return String.format("%0" + numberOfLeadingZeros + "d", number);
    }

    public static String toString(ZkClient zkClient) {
        return toString(zkClient, "/", PathFilter.ALL);
    }

    public static String toString(ZkClient zkClient, String startPath, PathFilter pathFilter) {
        final int level = 1;
        final StringBuilder builder = new StringBuilder("+ (" + startPath + ")");
        builder.append("\n");
        addChildrenToStringBuilder(zkClient, pathFilter, level, builder, startPath);
        return builder.toString();
    }

    private static void addChildrenToStringBuilder(ZkClient zkClient, PathFilter pathFilter, final int level, final StringBuilder builder, final String startPath) {
        final List<String> children = zkClient.getChildren(startPath);
        for (final String node : children) {
            String nestedPath;
            if (startPath.endsWith("/")) {
                nestedPath = startPath + node;
            } else {
                nestedPath = startPath + "/" + node;
            }
            if (pathFilter.showChilds(nestedPath)) {
                builder.append(getSpaces(level - 1) + "'-" + "+" + node + "\n");
                addChildrenToStringBuilder(zkClient, pathFilter, level + 1, builder, nestedPath);
            } else {
                builder.append(getSpaces(level - 1) + "'-" + "-" + node + " (contents hidden)\n");
            }
        }
    }

    private static String getSpaces(final int level) {
        String s = "";
        for (int i = 0; i < level; i++) {
            s += "  ";
        }
        return s;
    }

    public static interface PathFilter {

        public static PathFilter ALL = new PathFilter() {

            @Override
            public boolean showChilds(String path) {
                return true;
            }
        };

        boolean showChilds(String path);
    }

}