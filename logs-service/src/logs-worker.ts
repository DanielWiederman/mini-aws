import { Kafka } from 'kafkajs';
import { db } from './db.js';
import { LogEvent } from 'shared-contracts';
import { sql } from 'kysely';
import crypto from 'crypto';

const kafka = new Kafka({
  clientId: 'logs-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'logs-service-group' });

async function initDb() {
  await db.schema
    .createTable('system_logs')
    .ifNotExists()
    .addColumn('id', 'uuid', (col) => col.primaryKey())
    .addColumn('level', 'varchar(50)', (col) => col.notNull())
    .addColumn('service', 'varchar(255)', (col) => col.notNull())
    .addColumn('message', 'text', (col) => col.notNull())
    .addColumn('error_message', 'text')
    .addColumn('stack_trace', 'text')
    .addColumn('metadata', 'jsonb')
    .addColumn('timestamp', 'timestamp', (col) => col.notNull())
    .execute();
    
  console.log('📝 [Logs DB] Initialized tables');
}

async function run() {
  await initDb();
  await consumer.connect();
  await consumer.subscribe({ topic: 'system-logs-topic', fromBeginning: true });
  
  console.log('📝 [Logs Worker] Listening for logs on system-logs-topic...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        if (!message.value) return;
        
        const logEvent: LogEvent = JSON.parse(message.value.toString());
        
        // Bulk insert or single insert. For this example, we insert individually.
        // In extreme high scale, you'd batch these into chunks.
        await db.insertInto('system_logs').values({
          id: crypto.randomUUID(),
          level: logEvent.level,
          service: logEvent.service,
          message: logEvent.message,
          error_message: logEvent.error || null,
          stack_trace: logEvent.stack || null,
          metadata: logEvent.metadata ? JSON.stringify(logEvent.metadata) : null,
          timestamp: new Date(logEvent.timestamp)
        }).execute();

        if (logEvent.level === 'ERROR') {
          console.error(`🚨 [Log DB Sink] Recorded ERROR from ${logEvent.service}: ${logEvent.message}`);
        } else {
          console.log(`ℹ️ [Log DB Sink] Recorded ${logEvent.level} from ${logEvent.service}`);
        }

      } catch (e) {
        console.error('Failed to sink log to DB', e);
      }
    },
  });
}

run().catch(console.error);
