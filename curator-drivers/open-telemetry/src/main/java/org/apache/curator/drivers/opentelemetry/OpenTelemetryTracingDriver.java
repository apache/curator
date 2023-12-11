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

package org.apache.curator.drivers.opentelemetry;

import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.LATENCY_MS_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.PATH_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.REQUEST_BYTES_LENGTH_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.RESPONSE_BYTES_LENGTH_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.SESSION_ID_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_AVERSION_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_CTIME_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_CVERSION_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_CZXID_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_DATA_LENGTH_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_EPHEMERAL_OWNER_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_MTIME_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_MZXID_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_NUM_CHILDREN_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_PZXID_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.STAT_VERSION_ATTRIBUTE;
import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.WITH_WATCHER_ATTRIBUTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.apache.curator.drivers.AdvancedTracerDriver;
import org.apache.curator.drivers.EventTrace;
import org.apache.curator.drivers.OperationTrace;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/**
 * Converts Apache Curator traces and events to OpenTelemetry spans, and then publishes these to the supplied OTel
 * {@link Tracer}. Curator events are added to whatever OTel deems as the {@link Span#current()} current span}. Note
 * that in the case of attributes, this implementation takes the approach of capturing deviations from the baseline.
 * For example, it will not register attributes if they have the default value, only if the value has diverged from it.
 */
public class OpenTelemetryTracingDriver extends AdvancedTracerDriver {

    interface AttributeKeys {
        AttributeKey<String> PATH_ATTRIBUTE = AttributeKey.stringKey("path");
        AttributeKey<Long> SESSION_ID_ATTRIBUTE = AttributeKey.longKey("sessionId");
        AttributeKey<Long> LATENCY_MS_ATTRIBUTE = AttributeKey.longKey("latencyMs");
        AttributeKey<Long> REQUEST_BYTES_LENGTH_ATTRIBUTE = AttributeKey.longKey("requestBytesLength");
        AttributeKey<Long> RESPONSE_BYTES_LENGTH_ATTRIBUTE = AttributeKey.longKey("responseBytesLength");
        AttributeKey<Boolean> WITH_WATCHER_ATTRIBUTE = AttributeKey.booleanKey("withWatcher");
        AttributeKey<Long> STAT_AVERSION_ATTRIBUTE = AttributeKey.longKey("stat.Aversion");
        AttributeKey<Long> STAT_CTIME_ATTRIBUTE = AttributeKey.longKey("stat.Ctime");
        AttributeKey<Long> STAT_CVERSION_ATTRIBUTE = AttributeKey.longKey("stat.Cversion");
        AttributeKey<Long> STAT_CZXID_ATTRIBUTE = AttributeKey.longKey("stat.Czxid");
        AttributeKey<Long> STAT_DATA_LENGTH_ATTRIBUTE = AttributeKey.longKey("stat.DataLength");
        AttributeKey<Long> STAT_EPHEMERAL_OWNER_ATTRIBUTE = AttributeKey.longKey("stat.EphemeralOwner");
        AttributeKey<Long> STAT_MTIME_ATTRIBUTE = AttributeKey.longKey("stat.Mtime");
        AttributeKey<Long> STAT_MZXID_ATTRIBUTE = AttributeKey.longKey("stat.Mzxid");
        AttributeKey<Long> STAT_NUM_CHILDREN_ATTRIBUTE = AttributeKey.longKey("stat.NumChildren");
        AttributeKey<Long> STAT_PZXID_ATTRIBUTE = AttributeKey.longKey("stat.Pzxid");
        AttributeKey<Long> STAT_VERSION_ATTRIBUTE = AttributeKey.longKey("stat.Version");
    }

    private final Tracer tracer;

    public OpenTelemetryTracingDriver(Tracer tracer) {
        Preconditions.checkNotNull(tracer);
        this.tracer = tracer;
    }

    @Override
    public Object startTrace(OperationTrace trace) {
        Preconditions.checkNotNull(trace);
        Span span = tracer.spanBuilder(trace.getName())
                .setStartTimestamp(trace.getStartTimeNanos(), NANOSECONDS)
                .startSpan();

        if (trace.getPath() != null) {
            span.setAttribute(PATH_ATTRIBUTE, trace.getPath());
        }
        if (trace.isWithWatcher()) {
            span.setAttribute(WITH_WATCHER_ATTRIBUTE, true);
        }
        if (trace.getSessionId() >= 0) {
            span.setAttribute(SESSION_ID_ATTRIBUTE, trace.getSessionId());
        }
        if (trace.getRequestBytesLength() > 0) {
            span.setAttribute(REQUEST_BYTES_LENGTH_ATTRIBUTE, trace.getRequestBytesLength());
        }
        // We will close this at in endTrace
        @SuppressWarnings("MustBeClosedChecker")
        Scope scope = span.makeCurrent();
        return new SpanScope(span, scope);
    }

    @Override
    public void endTrace(OperationTrace trace) {
        Preconditions.checkNotNull(trace);
        SpanScope spanScope = (SpanScope) trace.getDriverTrace();
        Span span = spanScope.getSpan();

        if (trace.getReturnCode() == KeeperException.Code.OK.intValue()) {
            span.setStatus(StatusCode.OK);
        } else {
            span.setStatus(StatusCode.ERROR);
            span.recordException(
                    KeeperException.create(KeeperException.Code.get(trace.getReturnCode())),
                    Attributes.of(PATH_ATTRIBUTE, trace.getPath())
            );
        }

        span.setAttribute(LATENCY_MS_ATTRIBUTE, trace.getRequestBytesLength());

        if (trace.getResponseBytesLength() > 0) {
            span.setAttribute(RESPONSE_BYTES_LENGTH_ATTRIBUTE, trace.getResponseBytesLength());
        }

        if (trace.getStat() != null) {
            span.setAttribute(STAT_AVERSION_ATTRIBUTE, trace.getStat().getAversion());
            span.setAttribute(STAT_CTIME_ATTRIBUTE, trace.getStat().getCtime());
            span.setAttribute(STAT_CVERSION_ATTRIBUTE, trace.getStat().getCversion());
            span.setAttribute(STAT_CZXID_ATTRIBUTE, trace.getStat().getCzxid());
            span.setAttribute(STAT_DATA_LENGTH_ATTRIBUTE, trace.getStat().getDataLength());
            span.setAttribute(STAT_EPHEMERAL_OWNER_ATTRIBUTE, trace.getStat().getEphemeralOwner());
            span.setAttribute(STAT_MTIME_ATTRIBUTE, trace.getStat().getMtime());
            span.setAttribute(STAT_MZXID_ATTRIBUTE, trace.getStat().getMzxid());
            span.setAttribute(STAT_NUM_CHILDREN_ATTRIBUTE, trace.getStat().getNumChildren());
            span.setAttribute(STAT_PZXID_ATTRIBUTE, trace.getStat().getPzxid());
            span.setAttribute(STAT_VERSION_ATTRIBUTE, trace.getStat().getVersion());
        }

        span.end(trace.getEndTimeNanos(), NANOSECONDS);
        spanScope.getScope().close();
    }

    @Override
    public void addEvent(EventTrace trace) {
        Preconditions.checkNotNull(trace);
        Span span = Span.current();
        Attributes attrs;
        if (trace.getSessionId() >= 0) {
            attrs = Attributes.of(SESSION_ID_ATTRIBUTE, trace.getSessionId());
        } else {
            attrs = Attributes.empty();
        }
        span.addEvent(trace.getName(), attrs);
    }

    static final class SpanScope {
        private final Span span;
        private final Scope scope;

        SpanScope(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        Span getSpan() {
            return span;
        }

        Scope getScope() {
            return scope;
        }
    }
}
