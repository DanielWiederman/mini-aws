import { propagation, context, trace, TextMapSetter, TextMapGetter, SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { Producer, EachMessagePayload, IHeaders } from 'kafkajs';

const kafkaSetter: TextMapSetter<IHeaders> = {
  set(carrier, key, value) {
    carrier[key] = value;
  }
};

const kafkaGetter: TextMapGetter<IHeaders> = {
  keys(carrier) {
    return Object.keys(carrier);
  },
  get(carrier, key) {
    if (!carrier) return undefined;
    const value = carrier[key];
    if (Buffer.isBuffer(value)) {
      return value.toString('utf8');
    }
    if (Array.isArray(value)) {
      const v = value[0];
      return Buffer.isBuffer(v) ? v.toString('utf8') : (v as string);
    }
    return value as string;
  }
};

export async function sendTraced(producer: Producer, topic: string, messages: any[]) {
  const tracer = trace.getTracer('kafka-producer');
  
  return tracer.startActiveSpan(`produce ${topic}`, { kind: SpanKind.PRODUCER }, async (span) => {
    messages.forEach(m => {
      m.headers = m.headers || {};
      propagation.inject(context.active(), m.headers, kafkaSetter);
    });
    
    try {
      const res = await producer.send({ topic, messages });
      span.setStatus({ code: SpanStatusCode.OK });
      return res;
    } catch(err: any) {
      span.recordException(err);
      span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
      throw err;
    } finally {
      span.end();
    }
  });
}

export function tracedEachMessage(handler: (payload: EachMessagePayload) => Promise<void>) {
  const tracer = trace.getTracer('kafka-consumer');
  return async (payload: EachMessagePayload) => {
    const extractedContext = propagation.extract(context.active(), payload.message.headers || {}, kafkaGetter);
    
    return context.with(extractedContext, () => {
      return tracer.startActiveSpan(`consume ${payload.topic}`, { kind: SpanKind.CONSUMER }, async (span) => {
        try {
          await handler(payload);
          span.setStatus({ code: SpanStatusCode.OK });
        } catch(err: any) {
          span.recordException(err);
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
          throw err;
        } finally {
          span.end();
        }
      });
    });
  };
}
