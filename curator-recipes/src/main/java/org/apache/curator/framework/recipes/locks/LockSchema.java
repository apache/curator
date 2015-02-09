package org.apache.curator.framework.recipes.locks;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

public class LockSchema {
    private final Set<String> paths;

    public LockSchema() {
        paths = new HashSet<String>();
    }

    public LockSchema(Set<String> paths) {
        this.paths = Sets.newHashSet(paths);
    }

    public Set<String> getPaths() {
        return Sets.newHashSet(paths);
    }
}
