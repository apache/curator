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

import static org.apache.curator.drivers.opentelemetry.OpenTelemetryTracingDriver.AttributeKeys.SESSION_ID_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.curator.drivers.EventTrace;
import org.apache.curator.drivers.OperationTrace;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.StatusData;

class OpenTelemetryTracingDriverTest {
    @RegisterExtension
    static final OpenTelemetryExtension otel = OpenTelemetryExtension.create();

    private final Tracer tracer = otel.getOpenTelemetry().getTracer("test");
    private final OpenTelemetryTracingDriver driver = new OpenTelemetryTracingDriver(tracer);

    @Test
    void testAddTrace_ok() {
        OperationTrace trace = new OperationTrace("op", driver, 1L);
        trace.setPath("path");
        trace.setReturnCode(KeeperException.Code.OK.intValue());
        trace.commit();

        otel.assertTraces().hasTracesSatisfyingExactly(
                t -> t.hasSpansSatisfyingExactly(s -> {
                    s.hasName("op");
                    s.hasStatus(StatusData.ok());
                    s.hasAttribute(SESSION_ID_ATTRIBUTE, 1L);
                    s.hasKind(SpanKind.INTERNAL);
                    s.hasEnded();
                })
        );
    }

    @Test
    void testAddTrace_fail() {
        OperationTrace trace = new OperationTrace("op", driver);
        trace.setPath("path");
        trace.setReturnCode(KeeperException.Code.NOAUTH.intValue());
        trace.commit();

        otel.assertTraces().hasTracesSatisfyingExactly(
                t -> t.hasSpansSatisfyingExactly(s -> {
                    s.hasStatus(StatusData.error());
                    s.hasException(new KeeperException.NoAuthException());
                })
        );
    }

    @Test
    void testAddEvent() {
        Span span = tracer.spanBuilder("test-span").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            new EventTrace("session-expired", driver).commit();
            span.end();

            otel.assertTraces().hasTracesSatisfyingExactly(
                    t -> t.hasSpansSatisfyingExactly(
                            s -> s.hasEventsSatisfyingExactly(e -> e.hasName("session-expired"))
                    )
            );
        }
    }

    @Test
    void testAddEvent_noCurrentSpan() {
        new EventTrace("session-expired", driver).commit();
        otel.assertTraces().isEmpty();
    }

    @Test
    void testAddTraceAndEvent() {
        OperationTrace trace = new OperationTrace("op", driver, 1L);
        trace.setPath("path");

        new EventTrace("session-expired", driver, 1L).commit();

        trace.setReturnCode(KeeperException.Code.SESSIONEXPIRED.intValue());
        trace.commit();

        otel.assertTraces().hasTracesSatisfyingExactly(
                t -> t.hasSpansSatisfyingExactly(s -> {
                    s.hasStatus(StatusData.error());
                    s.hasEventsSatisfyingExactly(
                            e -> {
                                e.hasName("session-expired");
                                e.hasAttributesSatisfying(a -> {
                                    assertThat(a.get(SESSION_ID_ATTRIBUTE)).isEqualTo(1L);
                                    assertThat(a.size()).isEqualTo(1L);
                                });
                            },
                            e -> e.hasName("exception")
                    );
                })
        );
    }
}
