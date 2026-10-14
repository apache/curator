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

package org.apache.curator.x.discovery.details;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.x.discovery.ServiceInstance;

/**
 * A serializer that uses Jackson to serialize/deserialize as JSON. IMPORTANT: The instance
 * payload must support Jackson
 * <p>
 * The payload is serialized with its class name (see {@link ServiceInstance#getPayload()}). To prevent
 * deserialization of arbitrary classes, by default only the payload class and its subtypes are
 * accepted when deserializing the payload. Polymorphic types nested within the payload whose declared
 * base type is not {@link Object} are not restricted. Use {@link #JsonInstanceSerializer(Class, boolean, PolymorphicTypeValidator)}
 * to supply a custom {@link PolymorphicTypeValidator}.
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T> {
    private final ObjectMapper mapper;
    private final Class<T> payloadClass;
    private final boolean compatibleSerializationMode;
    private final JavaType type;

    /**
     * CURATOR-275 introduced a new field into {@link org.apache.curator.x.discovery.ServiceInstance}. This caused a potential
     * {@link com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException} in older clients that
     * read newly serialized ServiceInstances. Therefore the default behavior of JsonInstanceSerializer
     * has been changed to <em>NOT</em> serialize the <code>enabled</code> field. If you wish to use that field, use the
     * alternate constructor {@link #JsonInstanceSerializer(Class, boolean)} and pass true for
     * <code>compatibleSerializationMode</code>. Note: future versions of Curator <em>may</em> change this
     * behavior.
     *
     * @param payloadClass used to validate payloads when deserializing
     */
    public JsonInstanceSerializer(Class<T> payloadClass) {
        this(payloadClass, true, false);
    }

    /**
     * CURATOR-275 introduced a new field into {@link org.apache.curator.x.discovery.ServiceInstance}. This caused a potential
     * {@link com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException} in older clients that
     * read newly serialized ServiceInstances. If you are susceptible to this you should set the
     * serializer to be an instance of {@link org.apache.curator.x.discovery.details.JsonInstanceSerializer}
     * with <code>compatibleSerializationMode</code> set to true. IMPORTANT: when this is done, the new <code>enabled</code>
     * field of ServiceInstance is <strong>not</strong> serialized. If however you <em>do</em> want
     * to use the <code>enabled</code> field, set <code>compatibleSerializationMode</code> to false.
     *
     * @param payloadClass used to validate payloads when deserializing
     * @param compatibleSerializationMode pass true to serialize in a manner that supports clients pre-CURATOR-275
     */
    public JsonInstanceSerializer(Class<T> payloadClass, boolean compatibleSerializationMode) {
        this(payloadClass, compatibleSerializationMode, false);
    }

    /**
     * Same as {@link #JsonInstanceSerializer(Class, boolean)} but with a custom {@link PolymorphicTypeValidator}
     * that limits which classes may be deserialized as the payload.
     *
     * @param payloadClass used to validate payloads when deserializing
     * @param compatibleSerializationMode pass true to serialize in a manner that supports clients pre-CURATOR-275
     * @param polymorphicTypeValidator validates the payload class named in the serialized JSON
     */
    public JsonInstanceSerializer(
            Class<T> payloadClass,
            boolean compatibleSerializationMode,
            PolymorphicTypeValidator polymorphicTypeValidator) {
        this(payloadClass, compatibleSerializationMode, false, polymorphicTypeValidator);
    }

    @VisibleForTesting
    JsonInstanceSerializer(
            Class<T> payloadClass, boolean compatibleSerializationMode, boolean failOnUnknownProperties) {
        this(payloadClass, compatibleSerializationMode, failOnUnknownProperties, defaultValidator(payloadClass));
    }

    private JsonInstanceSerializer(
            Class<T> payloadClass,
            boolean compatibleSerializationMode,
            boolean failOnUnknownProperties,
            PolymorphicTypeValidator polymorphicTypeValidator) {
        this.payloadClass = payloadClass;
        this.compatibleSerializationMode = compatibleSerializationMode;
        mapper = new ObjectMapper();
        mapper.setPolymorphicTypeValidator(polymorphicTypeValidator);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknownProperties);
        type = mapper.getTypeFactory().constructType(ServiceInstance.class);
    }

    private static PolymorphicTypeValidator defaultValidator(Class<?> payloadClass) {
        return new PayloadTypeValidator(payloadClass);
    }

    /**
     * The payload is declared as {@link Object} (due to type erasure) so this validator only restricts
     * polymorphic values with an {@link Object} base type, which must be the payload class or a subtype of it.
     * Polymorphic values with any other base type are allowed (as they were before this validator was added).
     */
    private static class PayloadTypeValidator extends PolymorphicTypeValidator.Base {
        private static final long serialVersionUID = 1L;

        private final Class<?> payloadClass;

        PayloadTypeValidator(Class<?> payloadClass) {
            this.payloadClass = payloadClass;
        }

        @Override
        public Validity validateBaseType(MapperConfig<?> config, JavaType baseType) {
            return baseType.hasRawClass(Object.class) ? Validity.INDETERMINATE : Validity.ALLOWED;
        }

        @Override
        public Validity validateSubType(MapperConfig<?> config, JavaType baseType, JavaType subType) {
            return payloadClass.isAssignableFrom(subType.getRawClass()) ? Validity.ALLOWED : Validity.DENIED;
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        ServiceInstance rawServiceInstance = mapper.readValue(bytes, type);
        payloadClass.cast(rawServiceInstance.getPayload()); // just to verify that it's the correct type
        return (ServiceInstance<T>) rawServiceInstance;
    }

    @Override
    public byte[] serialize(ServiceInstance<T> instance) throws Exception {
        if (compatibleSerializationMode) {
            OldServiceInstance<T> compatible = new OldServiceInstance<T>(
                    instance.getName(),
                    instance.getId(),
                    instance.getAddress(),
                    instance.getPort(),
                    instance.getSslPort(),
                    instance.getPayload(),
                    instance.getRegistrationTimeUTC(),
                    instance.getServiceType(),
                    instance.getUriSpec());
            return mapper.writeValueAsBytes(compatible);
        }
        return mapper.writeValueAsBytes(instance);
    }
}
