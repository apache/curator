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

package org.apache.curator.framework.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

/**
 * Represents and documents operations allowed for a given path pattern
 */
public class Schema {
    private final String name;
    private final Pattern pathRegex;
    private final String fixedPath;
    private final String documentation;
    private final SchemaValidator schemaValidator;
    private final Allowance ephemeral;
    private final Allowance sequential;
    private final Allowance watched;
    private final boolean canBeDeleted;
    private final Map<String, String> metadata;

    public enum Allowance {
        CAN,
        MUST,
        CANNOT
    }

    /**
     * Start a builder for the given full path. Note: full path schemas
     * take precedence over regex path schemas.
     *
     * @param path full ZNode path. This schema only applies to an exact match
     * @return builder
     */
    public static SchemaBuilder builder(String path) {
        return new SchemaBuilder(null, path);
    }

    /**
     * Start a builder for the given path pattern.
     *
     * @param pathRegex regex for the path. This schema applies to any matching paths
     * @return builder
     */
    public static SchemaBuilder builder(Pattern pathRegex) {
        return new SchemaBuilder(pathRegex, null);
    }

    /**
     * Start a schema builder for a typical Curator recipe's parent node
     *
     * @param parentPath Path to the parent node
     * @return builder
     */
    public static SchemaBuilder builderForRecipeParent(String parentPath) {
        return new SchemaBuilder(null, parentPath).sequential(Allowance.CANNOT).ephemeral(Allowance.CANNOT);
    }

    /**
     * Start a schema builder for a typical Curator recipe's children
     *
     * @param parentPath Path to the parent node
     * @return builder
     */
    public static SchemaBuilder builderForRecipe(String parentPath) {
        return new SchemaBuilder(Pattern.compile(ZKPaths.makePath(parentPath, ".*")), null)
                .sequential(Allowance.MUST)
                .ephemeral(Allowance.MUST)
                .watched(Allowance.MUST)
                .canBeDeleted(true);
    }

    Schema(
            String name,
            Pattern pathRegex,
            String path,
            String documentation,
            SchemaValidator schemaValidator,
            Allowance ephemeral,
            Allowance sequential,
            Allowance watched,
            boolean canBeDeleted,
            Map<String, String> metadata) {
        Preconditions.checkArgument((pathRegex != null) || (path != null), "pathRegex and path cannot both be null");
        this.pathRegex = pathRegex;
        this.fixedPath = fixPath(path);
        this.metadata = ImmutableMap.copyOf(Preconditions.checkNotNull(metadata, "metadata cannot be null"));
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.documentation = Preconditions.checkNotNull(documentation, "documentation cannot be null");
        this.schemaValidator = Preconditions.checkNotNull(schemaValidator, "dataValidator cannot be null");
        this.ephemeral = Preconditions.checkNotNull(ephemeral, "ephemeral cannot be null");
        this.sequential = Preconditions.checkNotNull(sequential, "sequential cannot be null");
        this.watched = Preconditions.checkNotNull(watched, "watched cannot be null");
        this.canBeDeleted = canBeDeleted;
    }

    private String fixPath(String path) {
        if (path != null) {
            if (path.endsWith(ZKPaths.PATH_SEPARATOR)) {
                return (path.length() > 1) ? path.substring(0, path.length() - 1) : "";
            }
            return path;
        }
        return null;
    }

    /**
     * Validate that this schema allows znode deletion
     *
     * @param path the znode full path
     * @throws SchemaViolation if schema does not allow znode deletion
     */
    public void validateDelete(String path) {
        if (!canBeDeleted) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, null, null), "Cannot be deleted");
        }
    }

    /**
     * Validate that this schema's watching setting matches
     *
     * @param path the znode full path
     * @param isWatching true if attempt is being made to watch node
     * @throws SchemaViolation if schema's watching setting does not match
     */
    public void validateWatch(String path, boolean isWatching) {
        if (isWatching && (watched == Allowance.CANNOT)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, null, null), "Cannot be watched");
        }

        if (!isWatching && (watched == Allowance.MUST)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, null, null), "Must be watched");
        }
    }

    /**
     * Validate that this schema's create mode setting matches and that the data is valid
     *
     * @param mode CreateMode being used
     * @param path the znode full path
     * @param data data being set
     * @param acl the creation acls
     * @throws SchemaViolation if schema's create mode setting does not match or data is invalid
     */
    public void validateCreate(CreateMode mode, String path, byte[] data, List<ACL> acl) {
        if (mode.isEphemeral() && (ephemeral == Allowance.CANNOT)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, data, acl), "Cannot be ephemeral");
        }

        if (!mode.isEphemeral() && (ephemeral == Allowance.MUST)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, data, acl), "Must be ephemeral");
        }

        if (mode.isSequential() && (sequential == Allowance.CANNOT)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, data, acl), "Cannot be sequential");
        }

        if (!mode.isSequential() && (sequential == Allowance.MUST)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, data, acl), "Must be sequential");
        }

        validateGeneral(path, data, acl);
    }

    /**
     * Validate that this schema validates the data
     *
     *
     * @param path the znode full path
     * @param data data being set
     * @param acl if creating, the acls otherwise null or empty list
     * @throws SchemaViolation if data is invalid
     */
    public void validateGeneral(String path, byte[] data, List<ACL> acl) {
        if (!schemaValidator.isValid(this, path, data, acl)) {
            throw new SchemaViolation(this, new SchemaViolation.ViolatorData(path, data, acl), "Data is not valid");
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Return the raw path for this schema. If a full path was used, it is returned.
     * If a regex was used, it is returned
     *
     * @return path
     */
    public String getRawPath() {
        return (fixedPath != null) ? fixedPath : pathRegex.pattern();
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public Pattern getPathRegex() {
        return pathRegex;
    }

    public String getPath() {
        return fixedPath;
    }

    public String getDocumentation() {
        return documentation;
    }

    public SchemaValidator getSchemaValidator() {
        return schemaValidator;
    }

    public Allowance getEphemeral() {
        return ephemeral;
    }

    public Allowance getSequential() {
        return sequential;
    }

    public Allowance getWatched() {
        return watched;
    }

    public boolean canBeDeleted() {
        return canBeDeleted;
    }

    // intentionally only path and pathRegex
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Schema schema = (Schema) o;

        //noinspection SimplifiableIfStatement
        if (!pathRegex.equals(schema.pathRegex)) {
            return false;
        }
        return fixedPath.equals(schema.fixedPath);
    }

    // intentionally only path and pathRegex
    @Override
    public int hashCode() {
        int result = pathRegex.hashCode();
        result = 31 * result + fixedPath.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Schema{" + "name='"
                + name + '\'' + ", pathRegex="
                + pathRegex + ", path='"
                + fixedPath + '\'' + ", documentation='"
                + documentation + '\'' + ", dataValidator="
                + schemaValidator.getClass() + ", ephemeral="
                + ephemeral + ", sequential="
                + sequential + ", watched="
                + watched + ", canBeDeleted="
                + canBeDeleted + ", metadata="
                + metadata + '}';
    }

    public String toDocumentation() {
        String pathLabel = (pathRegex != null) ? "Path Regex: " : "Path: ";
        return "Name: " + name + '\n'
                + pathLabel + getRawPath() + '\n'
                + "Doc: " + documentation + '\n'
                + "Validator: " + schemaValidator.getClass().getSimpleName() + '\n'
                + "Meta: " + metadata + '\n'
                + String.format(
                        "ephemeral: %s | sequential: %s | watched: %s | canBeDeleted: %s",
                        ephemeral, sequential, watched, canBeDeleted)
                + '\n';
    }
}
