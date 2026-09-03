import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";


/**
 * Standard OTel wiring every Node service gets by depending on this target
 * (directly, or transitively via the `ts_service` macro).
 * Mirrors tools/observability/jvm/OtelBootstrap.kt.
 * */
export function initOtel(serviceName: string): NodeSDK {
	const sdk = new NodeSDK({
		resource: new Resource({
			[SemanticResourceAttributes.SERVICE_NAME]: serviceName,
		}),
		traceExporter: new OTLPTraceExporter({
			url: process.env.OTEL_EXPORTER_OTLP_ENPOINT ?? "http://otel-collector:4317",
		}),
	});

	sdk.start();
	return sdk;
}