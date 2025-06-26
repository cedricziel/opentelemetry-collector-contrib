// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This file contains benchmarks for the spanmetrics connector, specifically
// measuring the performance impact of the OTTL-based transformations feature.
//
// Benchmark Results (on Apple M1 Pro):
//
// BenchmarkConnectorConsumeTracesBasic-8           ~30,192 ns/op  (no transformations)
// BenchmarkConnectorWithSimpleTransformations-8   ~150,801 ns/op  (basic OTTL rules)
// BenchmarkConnectorWithComplexTransformations-8  ~483,288 ns/op  (complex OTTL expressions)
// BenchmarkGetSpanName-8                          ~1,966 ns/op    (with transformations)
// BenchmarkGetSpanNameWithoutTransformations-8    ~2.7 ns/op      (without transformations)
//
// Key insights:
// - Simple transformations add ~5x overhead
// - Complex transformations add ~16x overhead  
// - Individual span name transformations cost ~2μs per span
// - Most overhead comes from OTTL expression evaluation

package spanmetricsconnector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"
)

// BenchmarkConnectorConsumeTracesBasic benchmarks the core trace consumption without transformations
func BenchmarkConnectorConsumeTracesBasic(b *testing.B) {
	cfg := createDefaultConfig()

	conn, err := newConnector(zap.NewNop(), cfg, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}
	conn.metricsConsumer = consumertest.NewNop()

	traces := createTestTraces(100) // 100 spans per batch

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := conn.ConsumeTraces(context.Background(), traces)
		if err != nil {
			b.Fatalf("ConsumeTraces failed: %v", err)
		}
	}
}

// BenchmarkConnectorWithSimpleTransformations benchmarks with basic OTTL transformations
func BenchmarkConnectorWithSimpleTransformations(b *testing.B) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)
	config.Transformations = &Transformations{
		Rules: []AttributeRule{
			{
				Condition:  "span.kind == SPAN_KIND_SERVER",
				Attributes: []string{"http.route", "http.target"},
				Priority:   10,
			},
			{
				Condition:  "span.kind == SPAN_KIND_CLIENT", 
				Attributes: []string{"http.client.template", "http.url"},
				Priority:   5,
			},
			{
				Condition:  "true",
				Attributes: []string{"operation.name"},
				Priority:   1,
			},
		},
		FallbackToSpanName: true,
	}

	conn, err := newConnector(zap.NewNop(), config, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}
	conn.metricsConsumer = consumertest.NewNop()

	traces := createTestTraces(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := conn.ConsumeTraces(context.Background(), traces)
		if err != nil {
			b.Fatalf("ConsumeTraces failed: %v", err)
		}
	}
}

// BenchmarkConnectorWithComplexTransformations benchmarks with complex OTTL expressions
func BenchmarkConnectorWithComplexTransformations(b *testing.B) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)
	config.Transformations = &Transformations{
		Rules: []AttributeRule{
			{
				Condition: "span.kind == SPAN_KIND_SERVER and attributes[\"http.method\"] != nil and attributes[\"http.route\"] != nil",
				Template:  "{{.http_method}} {{.http_route}}",
				Priority:  100,
			},
			{
				Condition: "attributes[\"db.system\"] == \"postgresql\" and attributes[\"db.operation\"] != nil",
				Template:  "postgres {{.db_operation}} {{.db_collection_name}}",
				Priority:  90,
			},
			{
				Condition: "span.status.code == STATUS_CODE_ERROR and attributes[\"error.type\"] != nil",
				Template:  "ERROR {{.error_type}}",
				Priority:  80,
			},
			{
				Condition: "attributes[\"messaging.system\"] != nil and attributes[\"messaging.destination.name\"] != nil",
				Template:  "{{.messaging_system}} {{.messaging_destination_name}}",
				Priority:  70,
			},
			{
				Condition:  "true",
				Attributes: []string{"operation.name"},
				Priority:   1,
			},
		},
		FallbackToSpanName: true,
	}

	conn, err := newConnector(zap.NewNop(), config, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}
	conn.metricsConsumer = consumertest.NewNop()

	traces := createTestTracesWithVariedData(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := conn.ConsumeTraces(context.Background(), traces)
		if err != nil {
			b.Fatalf("ConsumeTraces failed: %v", err)
		}
	}
}

// BenchmarkGetSpanName benchmarks just the span name transformation logic
func BenchmarkGetSpanName(b *testing.B) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)
	config.Transformations = &Transformations{
		Rules: []AttributeRule{
			{
				Condition: "span.kind == SPAN_KIND_SERVER and attributes[\"http.method\"] != nil and attributes[\"http.route\"] != nil",
				Template:  "{{.http_method}} {{.http_route}}",
				Priority:  100,
			},
			{
				Condition:  "span.kind == SPAN_KIND_SERVER",
				Attributes: []string{"http.route", "http.target"},
				Priority:   50,
			},
			{
				Condition:  "true",
				Attributes: []string{"operation.name"},
				Priority:   1,
			},
		},
		FallbackToSpanName: true,
	}

	conn, err := newConnector(zap.NewNop(), config, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}

	span := createSpanWithAttributes("HTTP GET", ptrace.SpanKindServer, map[string]string{
		"http.method": "GET",
		"http.route":  "/api/users/{id}",
		"http.target": "/api/users/123",
	})
	resourceAttrs := createResourceAttrs(map[string]string{
		"service.name": "user-service",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = conn.getSpanName(span, resourceAttrs)
	}
}

// BenchmarkGetSpanNameWithoutTransformations benchmarks span name retrieval without transformations
func BenchmarkGetSpanNameWithoutTransformations(b *testing.B) {
	cfg := createDefaultConfig()
	conn, err := newConnector(zap.NewNop(), cfg, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}

	span := createSpanWithAttributes("HTTP GET", ptrace.SpanKindServer, map[string]string{
		"http.method": "GET",
		"http.route":  "/api/users/{id}",
	})
	resourceAttrs := createResourceAttrs(map[string]string{
		"service.name": "user-service",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = conn.getSpanName(span, resourceAttrs)
	}
}

// Helper functions for creating test data

func createTestTraces(spanCount int) ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	
	// Add resource attributes
	rs.Resource().Attributes().PutStr(string(conventions.ServiceNameKey), "test-service")
	rs.Resource().Attributes().PutStr("service.version", "1.0.0")
	
	ss := rs.ScopeSpans().AppendEmpty()
	
	for i := 0; i < spanCount; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("operation-%d", i))
		span.SetKind(ptrace.SpanKindServer)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(100 * time.Millisecond)))
		
		// Add some basic attributes
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutStr("http.route", fmt.Sprintf("/api/resource/%d", i))
		span.Attributes().PutInt("http.status_code", 200)
	}
	
	return traces
}

func createTestTracesWithVariedData(spanCount int) ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	
	rs.Resource().Attributes().PutStr(string(conventions.ServiceNameKey), "mixed-service")
	ss := rs.ScopeSpans().AppendEmpty()
	
	for i := 0; i < spanCount; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Duration(i+1) * time.Millisecond)))
		
		switch i % 4 {
		case 0: // HTTP server spans
			span.SetName("HTTP Request")
			span.SetKind(ptrace.SpanKindServer)
			span.Attributes().PutStr("http.method", "GET")
			span.Attributes().PutStr("http.route", fmt.Sprintf("/api/users/%d", i))
			span.Attributes().PutInt("http.status_code", 200)
		case 1: // Database spans
			span.SetName("DB Query")
			span.SetKind(ptrace.SpanKindClient)
			span.Attributes().PutStr("db.system", "postgresql")
			span.Attributes().PutStr("db.operation", "SELECT")
			span.Attributes().PutStr("db.collection.name", "users")
		case 2: // Messaging spans
			span.SetName("Message Send")
			span.SetKind(ptrace.SpanKindProducer)
			span.Attributes().PutStr("messaging.system", "kafka")
			span.Attributes().PutStr("messaging.destination.name", "user-events")
		case 3: // Error spans
			span.SetName("Failed Operation")
			span.SetKind(ptrace.SpanKindInternal)
			span.Status().SetCode(ptrace.StatusCodeError)
			span.Attributes().PutStr("error.type", "TimeoutError")
		}
	}
	
	return traces
}

func createSpanWithAttributes(name string, kind ptrace.SpanKind, attrs map[string]string) ptrace.Span {
	span := ptrace.NewSpan()
	span.SetName(name)
	span.SetKind(kind)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(100 * time.Millisecond)))
	
	for k, v := range attrs {
		span.Attributes().PutStr(k, v)
	}
	
	return span
}

func createResourceAttrs(attrs map[string]string) pcommon.Map {
	resourceAttrs := pcommon.NewMap()
	for k, v := range attrs {
		resourceAttrs.PutStr(k, v)
	}
	return resourceAttrs
}

// BenchmarkGetSpanNameOptimized benchmarks the optimized span name logic with fast paths
func BenchmarkGetSpanNameOptimized(b *testing.B) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)
	config.Transformations = &Transformations{
		Rules: []AttributeRule{
			// Simple span kind rule - should hit fast path
			{
				Condition:  "span.kind == SPAN_KIND_SERVER",
				Attributes: []string{"http.route", "http.target"},
				Priority:   100,
			},
			// Universal rule - should hit fast path
			{
				Condition:  "true",
				Attributes: []string{"operation.name"},
				Priority:   1,
			},
		},
		FallbackToSpanName: true,
	}

	conn, err := newConnector(zap.NewNop(), config, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}

	span := createSpanWithAttributes("HTTP GET", ptrace.SpanKindServer, map[string]string{
		"http.route": "/api/users/{id}",
		"http.target": "/api/users/123",
	})
	resourceAttrs := createResourceAttrs(map[string]string{
		"service.name": "user-service",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = conn.getSpanName(span, resourceAttrs)
	}
}

// BenchmarkGetSpanNameComplex benchmarks complex OTTL expressions
func BenchmarkGetSpanNameComplex(b *testing.B) {
	cfg := createDefaultConfig()
	config := cfg.(*Config)
	config.Transformations = &Transformations{
		Rules: []AttributeRule{
			// Complex condition requiring OTTL evaluation
			{
				Condition: "span.kind == SPAN_KIND_SERVER and attributes[\"http.method\"] != nil and IsMatch(attributes[\"http.target\"], \"^/api/.*\")",
				Template:  "{{.http_method}} {{.http_route}}",
				Priority:  100,
			},
		},
		FallbackToSpanName: true,
	}

	conn, err := newConnector(zap.NewNop(), config, clockwork.NewFakeClock())
	if err != nil {
		b.Fatalf("Failed to create connector: %v", err)
	}

	span := createSpanWithAttributes("HTTP GET", ptrace.SpanKindServer, map[string]string{
		"http.method": "GET",
		"http.route":  "/api/users/{id}",
		"http.target": "/api/users/123",
	})
	resourceAttrs := createResourceAttrs(map[string]string{
		"service.name": "user-service",
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = conn.getSpanName(span, resourceAttrs)
	}
}