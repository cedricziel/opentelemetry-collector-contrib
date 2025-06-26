// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"

import (
	"errors"
	"fmt"
	"text/template"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

const (
	delta      = "AGGREGATION_TEMPORALITY_DELTA"
	cumulative = "AGGREGATION_TEMPORALITY_CUMULATIVE"
)

var defaultHistogramBucketsMs = []float64{
	2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000,
}

var defaultDeltaTimestampCacheSize = 1000

// Dimension defines the dimension name and optional default value if the Dimension is missing from a span attribute.
type Dimension struct {
	Name    string  `mapstructure:"name"`
	Default *string `mapstructure:"default"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// AttributeRule defines a rule for selecting span name based on span characteristics
type AttributeRule struct {
	// Condition allows complex matching using OTTL expressions for rule evaluation
	// Example: 'span.kind == SPAN_KIND_SERVER and attributes["http.route"] != nil'
	// Example: 'attributes["db.system"] == "postgresql" and IsMatch(name, ".*query.*")'
	Condition string `mapstructure:"condition"`
	// Attributes specifies a list of attribute names to try in order (first non-empty wins)
	Attributes []string `mapstructure:"attributes"`
	// Template allows combining multiple attributes using Go template syntax
	// Example: "{{.http_method}} {{.http_route}}" or "{{.http_method}} {{.http_target}}"
	Template string `mapstructure:"template"`
	// Priority defines the order of precedence (higher values have higher priority)
	Priority int `mapstructure:"priority"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// Transformations defines rules for selecting span name attribute based on span characteristics
type Transformations struct {
	// Rules defines the list of rules for selecting span name attribute
	Rules []AttributeRule `mapstructure:"rules"`
	// Fallback to span.Name() if no rules match or attributes are found
	FallbackToSpanName bool `mapstructure:"fallback_to_span_name"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// Config defines the configuration options for spanmetricsconnector.
type Config struct {
	// Dimensions defines the list of additional dimensions on top of the provided:
	// - service.name
	// - span.kind
	// - span.kind
	// - status.code
	// The dimensions will be fetched from the span's attributes. Examples of some conventionally used attributes:
	// https://github.com/open-telemetry/opentelemetry-collector/blob/main/model/semconv/opentelemetry.go.
	Dimensions        []Dimension `mapstructure:"dimensions"`
	CallsDimensions   []Dimension `mapstructure:"calls_dimensions"`
	ExcludeDimensions []string    `mapstructure:"exclude_dimensions"`

	// DimensionsCacheSize defines the size of cache for storing Dimensions, which helps to avoid cache memory growing
	// indefinitely over the lifetime of the collector.
	// Optional. See defaultDimensionsCacheSize in connector.go for the default value.
	// Deprecated:  Please use AggregationCardinalityLimit instead
	DimensionsCacheSize int `mapstructure:"dimensions_cache_size"`

	// ResourceMetricsCacheSize defines the size of the cache holding metrics for a service. This is mostly relevant for
	// cumulative temporality to avoid memory leaks and correct metric timestamp resets.
	// Optional. See defaultResourceMetricsCacheSize in connector.go for the default value.
	ResourceMetricsCacheSize int `mapstructure:"resource_metrics_cache_size"`

	// ResourceMetricsKeyAttributes filters the resource attributes used to create the resource metrics key hash.
	// This can be used to avoid situations where resource attributes may change across service restarts, causing
	// metric counters to break (and duplicate). A resource does not need to have all of the attributes. The list
	// must include enough attributes to properly identify unique resources or risk aggregating data from more
	// than one service and span.
	// e.g. ["service.name", "telemetry.sdk.language", "telemetry.sdk.name"]
	// See https://opentelemetry.io/docs/specs/semconv/resource/ for possible attributes.
	ResourceMetricsKeyAttributes []string `mapstructure:"resource_metrics_key_attributes"`

	AggregationTemporality string `mapstructure:"aggregation_temporality"`

	Histogram HistogramConfig `mapstructure:"histogram"`

	// MetricsEmitInterval is the time period between when metrics are flushed or emitted to the configured MetricsExporter.
	MetricsFlushInterval time.Duration `mapstructure:"metrics_flush_interval"`

	// MetricsExpiration is the time period after which, if no new spans are received, metrics are considered stale and will no longer be exported.
	// Default value (0) means that the metrics will never expire.
	MetricsExpiration time.Duration `mapstructure:"metrics_expiration"`

	// TimestampCacheSize controls the size of the cache used to keep track of delta metrics' TimestampUnixNano the last time it was flushed
	TimestampCacheSize *int `mapstructure:"metric_timestamp_cache_size"`

	// Namespace is the namespace of the metrics emitted by the connector.
	Namespace string `mapstructure:"namespace"`

	// Exemplars defines the configuration for exemplars.
	Exemplars ExemplarsConfig `mapstructure:"exemplars"`

	// Events defines the configuration for events section of spans.
	Events EventsConfig `mapstructure:"events"`

	IncludeInstrumentationScope []string `mapstructure:"include_instrumentation_scope"`

	AggregationCardinalityLimit int `mapstructure:"aggregation_cardinality_limit"`

	// Transformations defines rules for selecting span name attribute based on span characteristics
	Transformations *Transformations `mapstructure:"transformations"`
}

type HistogramConfig struct {
	Disable     bool                        `mapstructure:"disable"`
	Unit        metrics.Unit                `mapstructure:"unit"`
	Exponential *ExponentialHistogramConfig `mapstructure:"exponential"`
	Explicit    *ExplicitHistogramConfig    `mapstructure:"explicit"`
	Dimensions  []Dimension                 `mapstructure:"dimensions"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type ExemplarsConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	MaxPerDataPoint *int `mapstructure:"max_per_data_point"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type ExponentialHistogramConfig struct {
	MaxSize int32 `mapstructure:"max_size"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type ExplicitHistogramConfig struct {
	// Buckets is the list of durations representing explicit histogram buckets.
	Buckets []time.Duration `mapstructure:"buckets"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type EventsConfig struct {
	// Enabled is a flag to enable events.
	Enabled bool `mapstructure:"enabled"`
	// Dimensions defines the list of dimensions to add to the events metric.
	Dimensions []Dimension `mapstructure:"dimensions"`
	// prevent unkeyed literal initialization
	_ struct{}
}

var _ xconfmap.Validator = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (c Config) Validate() error {
	if err := validateDimensions(c.Dimensions); err != nil {
		return fmt.Errorf("failed validating dimensions: %w", err)
	}
	if err := validateEventDimensions(c.Events.Enabled, c.Events.Dimensions); err != nil {
		return fmt.Errorf("failed validating event dimensions: %w", err)
	}

	if c.Histogram.Explicit != nil && c.Histogram.Exponential != nil {
		return errors.New("use either `explicit` or `exponential` buckets histogram")
	}

	if c.MetricsFlushInterval < 0 {
		return fmt.Errorf("invalid metrics_flush_interval: %v, the duration should be positive", c.MetricsFlushInterval)
	}

	if c.MetricsExpiration < 0 {
		return fmt.Errorf("invalid metrics_expiration: %v, the duration should be positive", c.MetricsExpiration)
	}

	if c.GetAggregationTemporality() == pmetric.AggregationTemporalityDelta && c.GetDeltaTimestampCacheSize() <= 0 {
		return fmt.Errorf(
			"invalid delta timestamp cache size: %v, the maximum number of the items in the cache should be positive",
			c.GetDeltaTimestampCacheSize(),
		)
	}

	if c.AggregationCardinalityLimit < 0 {
		return fmt.Errorf("invalid aggregation_cardinality_limit: %v, the limit should be positive", c.AggregationCardinalityLimit)
	}

	if err := validateTransformations(c.Transformations); err != nil {
		return fmt.Errorf("failed validating transformations: %w", err)
	}

	return nil
}

// GetAggregationTemporality converts the string value given in the config into a AggregationTemporality.
// Returns cumulative, unless delta is correctly specified.
func (c Config) GetAggregationTemporality() pmetric.AggregationTemporality {
	if c.AggregationTemporality == delta {
		return pmetric.AggregationTemporalityDelta
	}
	return pmetric.AggregationTemporalityCumulative
}

func (c Config) GetDeltaTimestampCacheSize() int {
	if c.TimestampCacheSize != nil {
		return *c.TimestampCacheSize
	}
	return defaultDeltaTimestampCacheSize
}

// validateDimensions checks duplicates for reserved dimensions and additional dimensions.
func validateDimensions(dimensions []Dimension) error {
	labelNames := make(map[string]struct{})
	for _, key := range []string{serviceNameKey, spanKindKey, statusCodeKey, spanNameKey} {
		labelNames[key] = struct{}{}
	}

	for _, key := range dimensions {
		if _, ok := labelNames[key.Name]; ok {
			return fmt.Errorf("duplicate dimension name %s", key.Name)
		}
		labelNames[key.Name] = struct{}{}
	}

	return nil
}

// validateEventDimensions checks for empty and duplicates for the dimensions configured.
func validateEventDimensions(enabled bool, dimensions []Dimension) error {
	if !enabled {
		return nil
	}
	if len(dimensions) == 0 {
		return errors.New("no dimensions configured for events")
	}
	return validateDimensions(dimensions)
}

// validateTransformations validates the transformations configuration
func validateTransformations(transformations *Transformations) error {
	if transformations == nil {
		return nil
	}

	if len(transformations.Rules) == 0 {
		return errors.New("transformations requires at least one rule")
	}


	for i, rule := range transformations.Rules {
		// Rule must have a condition
		if rule.Condition == "" {
			return fmt.Errorf("rule at index %d must have 'condition' set", i)
		}

		// Rule must have exactly one action method
		actionMethods := 0
		if len(rule.Attributes) > 0 {
			actionMethods++
		}
		if rule.Template != "" {
			actionMethods++
		}

		if actionMethods == 0 {
			return fmt.Errorf("rule at index %d must have either 'attributes' or 'template' set", i)
		}
		if actionMethods > 1 {
			return fmt.Errorf("rule at index %d cannot have both 'attributes' and 'template' set", i)
		}

		// Validate template syntax if provided
		if rule.Template != "" {
			if err := validateTemplateFormat(rule.Template); err != nil {
				return fmt.Errorf("rule at index %d has invalid template: %w", i, err)
			}
		}

		// Validate OTTL condition syntax if provided
		if rule.Condition != "" {
			if err := validateOTTLCondition(rule.Condition); err != nil {
				return fmt.Errorf("rule at index %d has invalid OTTL condition: %w", i, err)
			}
		}
	}

	return nil
}

// validateTemplateFormat validates that the template string is valid Go template syntax
func validateTemplateFormat(tmpl string) error {
	_, err := template.New("span_name").Parse(tmpl)
	return err
}

// validateOTTLCondition validates that the OTTL condition string is valid
func validateOTTLCondition(condition string) error {
	ottlParser, err := ottlspan.NewParser(
		ottlfuncs.StandardFuncs[ottlspan.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to create OTTL parser: %w", err)
	}
	
	_, err = ottlParser.ParseCondition(condition)
	if err != nil {
		return fmt.Errorf("invalid OTTL condition syntax: %w", err)
	}
	
	return nil
}
