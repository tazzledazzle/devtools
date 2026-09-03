//! Standard OTel wiring every Rust service gets by depending on this
//! crate — mirrors bootstrap.go / OtelBootstrap.kt / otel-bootstrap.ts.

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace as sdktrace, Resource};

pub fn init(service_name: &str) -> sdktrace::Tracer {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://otel-collector:4317".to_string());

    let resource = Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        service_name.to_string(),
    )]);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .with_trace_config(sdktrace::config().with_resource(resource))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("failed to install OTel pipeline");

    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());
    tracer
}