package tools.observability.jvm

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
 
/**
 * Standard OTel wiring every JVM service gets by depending on this
 * target (directly, or transitively via the `kt_service` macro).
 *
 * The point isn't that this is fancy — it's that no team has to think
 * about exporter config, resource attributes, or SDK versions. They
 * call `OtelBootstrap.init(serviceName)` and they're on the paved road.
 */
object OtelBootstrap {

	fun init(serviceName: String): OpenTelemetry {
		val resource = Resource.getDefault().merge(
			Resource.create(
				Attributes.of(
					io.opentelemetry.semconv.ResourceAttributes.SERVICE_NAME,
					serviceName,
				),
			),
		)

		val tracerProvider = SdkTracerProvider.builder()
			.addSpanProcessor(
				BatchSpanProcessor.builder(
					OtlpGrpcSpanExporter.builder()
						.setEndpoint(otlpEndpoint())
						.build(),
				).build(),
			)
			.setResources(resource)
			.build()

		return OpenTelemetrySdk.builder()
			.setTracerProvider(tracerProvider)
			.buildAndRegisterGlobal()
	}

	private fun otlpEndpoint(): String = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") ?: "http://otel-collector:4317"
}