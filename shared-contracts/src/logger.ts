import { Producer } from 'kafkajs';

export interface LogEvent {
  level: 'INFO' | 'WARN' | 'ERROR';
  service: string;
  message: string;
  error?: string;
  stack?: string;
  metadata?: any;
  timestamp: string;
}

export class KafkaLogger {
  constructor(private producer: Producer, private serviceName: string) {}

  async error(message: string, error?: any, metadata?: any) {
    await this.emit('ERROR', message, error, metadata);
  }

  async info(message: string, metadata?: any) {
    await this.emit('INFO', message, undefined, metadata);
  }

  async warn(message: string, metadata?: any) {
    await this.emit('WARN', message, undefined, metadata);
  }

  private async emit(level: 'INFO' | 'WARN' | 'ERROR', message: string, error?: any, metadata?: any) {
    try {
      const logEvent: LogEvent = {
        level,
        service: this.serviceName,
        message,
        error: error?.message || (typeof error === 'string' ? error : undefined),
        stack: error?.stack,
        metadata,
        timestamp: new Date().toISOString()
      };

      await this.producer.send({
        topic: 'system-logs-topic',
        messages: [{ value: JSON.stringify(logEvent) }]
      });
    } catch (e) {
      // Fallback to console if Kafka logger fails, to prevent infinite crash loops
      console.error(`[KafkaLogger] Failed to send log to Kafka:`, e);
      console.error(`[Original Error]`, error);
    }
  }
}
