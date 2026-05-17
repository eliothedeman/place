// Package obs owns the placefs observability surface that goes beyond the
// /metrics + /healthz on the admin server — today, OpenTelemetry tracing.
//
// The default value is "tracing off." Init returns a no-op shutdown when no
// endpoint is configured, and the rest of the codebase uses Tracer() which
// always returns a valid tracer (noop when uninitialised). That way the
// instrumentation in fuselayer / store / index can call Start unconditionally
// and pay only the noop-Start cost (a few ns) when tracing is disabled.
//
// Why OpenTelemetry + OTLP/HTTP: Jaeger has accepted OTLP natively since
// v1.35, and the HTTP exporter avoids pulling gRPC into the build. The same
// endpoint works for Tempo, Honeycomb, Datadog OTLP, etc., so the operator
// can swap the collector without touching the binary.
package obs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation-library name carried on every span.
// Anything that calls Tracer() shares this name; per-span detail goes in
// the span name + attributes, not in the tracer name.
const tracerName = "github.com/eliothedeman/place"

// Config drives Init. Zero value means "tracing disabled."
type Config struct {
	// Endpoint is the OTLP/HTTP receiver — e.g. "http://jaeger:4318",
	// "https://collector.example.com", or a bare "jaeger:4318". Empty
	// disables tracing entirely.
	Endpoint string

	// ServiceName tags every span. Defaults to "placefs" when empty.
	ServiceName string

	// ServiceVersion is optional; surfaced as service.version on the
	// resource so deploys are distinguishable in the UI.
	ServiceVersion string

	// SampleRatio is the head-based sampling probability, [0, 1]. 0 turns
	// every local-root span into a no-op (parents-respected); 1 records
	// every span. The write path fires many spans/sec under load — keep
	// this low (≤0.01) in steady state and crank it up while you're
	// actively debugging.
	SampleRatio float64

	// Insecure, when set with an https:// endpoint, downgrades the
	// connection to plaintext. Bare host:port and http:// are already
	// plaintext and don't need this flag.
	Insecure bool

	// BatchTimeout caps how long the SDK buffers spans before flushing.
	// Defaults to 5s — short enough that Jaeger shows traces during an
	// interactive debugging session without spamming the exporter.
	BatchTimeout time.Duration
}

// Init configures the global tracer provider from cfg. If cfg.Endpoint is
// empty the function is a no-op (returns a no-op shutdown) so callers can
// always defer the returned closer without special-casing "tracing off."
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	if cfg.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	if cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return nil, fmt.Errorf("obs: SampleRatio %g out of [0,1]", cfg.SampleRatio)
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "placefs"
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = 5 * time.Second
	}

	hostPort, insecure, err := parseEndpoint(cfg.Endpoint, cfg.Insecure)
	if err != nil {
		return nil, err
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(hostPort)}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exp, err := otlptrace.New(ctx, otlptracehttp.NewClient(opts...))
	if err != nil {
		return nil, fmt.Errorf("obs: build OTLP exporter: %w", err)
	}

	attrs := []attribute.KeyValue{semconv.ServiceName(cfg.ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}
	res, resErr := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(attrs...),
	)
	if resErr != nil {
		// resource.New only returns errors when WithFromEnv hits malformed
		// OTEL_RESOURCE_ATTRIBUTES — fall back to a minimal resource rather
		// than refusing to start. Tracing should never block the data plane.
		res = resource.NewSchemaless(attrs...)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(cfg.BatchTimeout)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		// Bound the flush so a hung collector can't block SIGTERM forever.
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return errors.Join(tp.Shutdown(shutdownCtx), exp.Shutdown(shutdownCtx))
	}, nil
}

// Tracer returns the placefs tracer. Safe to call before Init — it
// resolves through otel's global provider, which is the noop provider
// until Init swaps in the SDK one. Hot-path callers therefore pay only
// the noop-Start cost when tracing is disabled.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// parseEndpoint splits a user-supplied endpoint into the host:port that
// the OTLP/HTTP exporter wants plus a TLS toggle. Accepts bare host:port
// (defaults to plaintext), http://… (plaintext), and https://… (TLS).
func parseEndpoint(raw string, insecureOverride bool) (hostPort string, insecure bool, err error) {
	if !strings.Contains(raw, "://") {
		return raw, true, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", false, fmt.Errorf("obs: parse endpoint %q: %w", raw, err)
	}
	switch u.Scheme {
	case "http":
		return u.Host, true, nil
	case "https":
		return u.Host, insecureOverride, nil
	default:
		return "", false, fmt.Errorf("obs: unsupported scheme %q (use http or https)", u.Scheme)
	}
}
