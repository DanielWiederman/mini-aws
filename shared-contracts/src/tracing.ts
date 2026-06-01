import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { KafkaJsInstrumentation } from '@opentelemetry/instrumentation-kafkajs';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { ExpressInstrumentation } from '@opentelemetry/instrumentation-express';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';

export function initTracing(serviceName: string) {
  const sdk = new NodeSDK({
    serviceName,
    traceExporter: new OTLPTraceExporter({
      url: 'http://localhost:4318/v1/traces',
    }),
    instrumentations: [
      new HttpInstrumentation(),
      new ExpressInstrumentation(),
      new PgInstrumentation()
    ],
  });

  sdk.start();
  
  process.on('SIGTERM', () => {
    sdk.shutdown().finally(() => process.exit(0));
  });
  
  console.log(`🔎 [Tracing] OpenTelemetry initialized for ${serviceName}`);
}
