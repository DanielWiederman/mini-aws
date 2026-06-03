import { Kafka, Partitioners } from 'kafkajs';
import { sql } from 'kysely';
import { db } from './db.js';
import { OrdersModel } from './orders-model.js';
import { OrderCommand, CreateOrderCommandPayload, OrderEvent, tracedEachMessage, KafkaLogger, withDLQ } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'orders-service-worker',
  brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')]
});

const consumer = kafka.consumer({ 
  groupId: 'orders-service-group',
  maxInFlightRequests: 30,
  allowAutoTopicCreation: false
});
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  allowAutoTopicCreation: true
});
const sysLogger = new KafkaLogger(producer, 'orders-service');
const ordersModel = new OrdersModel(producer, sysLogger);

async function initDB() {
  await db.schema
    .createTable('order')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('order_id', 'varchar', (col) => col.notNull().unique())
    .addColumn('customer_id', 'varchar', (col) => col.notNull())
    .addColumn('status', 'varchar', (col) => col.notNull())
    .addColumn('stock_status', 'varchar', (col) => col.notNull())
    .addColumn('customer_status', 'varchar', (col) => col.notNull())
    .addColumn('created_at', 'timestamp', (col) => col.defaultTo(sql`now()`).notNull())
    .addColumn('updated_at', 'timestamp', (col) => col.defaultTo(sql`now()`).notNull())
    .execute();
    
  await db.schema
    .createTable('order_item')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('order_id', 'varchar', (col) => col.notNull())
    .addColumn('product_id', 'varchar', (col) => col.notNull())
    .addColumn('quantity', 'integer', (col) => col.notNull())
    .execute();

  await db.schema
    .createTable('outbox')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('topic', 'varchar', (col) => col.notNull())
    .addColumn('key', 'varchar')
    .addColumn('payload', 'jsonb', (col) => col.notNull())
    .addColumn('event_id', 'varchar')
    .addColumn('created_at', 'timestamp', (col) => col.defaultTo(sql`now()`).notNull())
    .addColumn('processed_at', 'timestamp')
    .execute();
    
  console.log('🛒 [Orders DB] Initialized tables');
}

async function start() {
  await initDB();
  await producer.connect();
  await consumer.connect();
  
  // Listen to BOTH commands from gateway and saga responses from other services
  await consumer.subscribe({ topic: 'orders-commands-topic', fromBeginning: false });
  await consumer.subscribe({ topic: 'orders-topic', fromBeginning: false });
  console.log('🛒 [Orders Worker] Listening for commands and saga responses');

  const dlqRetryMap = new Map<string, number>();

  await consumer.run({
    eachMessage: tracedEachMessage(async ({ topic, partition, message }) => {
      if (!message.value) return;
      
      await withDLQ(producer, topic, partition, message.offset, message, async () => {
        if (topic === 'orders-commands-topic') {
          const command: OrderCommand = JSON.parse(message.value!.toString());
          if (command.commandType === 'CREATE_ORDER_START') {
            await ordersModel.createPendingOrder(command.payload as CreateOrderCommandPayload);
          }
        } else if (topic === 'orders-topic') {
          const event: OrderEvent = JSON.parse(message.value!.toString());
          if (!event.eventType) return;
          
          if (['STOCK_RESERVED_END', 'STOCK_DENIED_END', 'CUSTOMER_VALIDATED_END', 'CUSTOMER_INVALID_END'].includes(event.eventType)) {
            console.log(`🛒 [Orders Worker] Received saga response: ${event.eventType} for ${event.orderId}`);
            await ordersModel.handleSagaResponse(event.orderId, event.eventType as any);
          }
        }
      }, dlqRetryMap);
    }),
  });

  // Outbox Polling Relay for Crash Recovery
  const intervalId = setInterval(async () => {
    try {
      await ordersModel.flushOutbox();
    } catch (e) {
      console.error('🛒 [Outbox Relay] Error during outbox sweep', e);
    }
  }, 5000);

  const shutdown = async () => {
    console.log('🛒 [Orders Worker] Shutting down gracefully...');
    clearInterval(intervalId);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

start().catch(console.error);
