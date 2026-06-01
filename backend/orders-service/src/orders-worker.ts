import { Kafka, Partitioners } from 'kafkajs';
import { sql } from 'kysely';
import { db } from './db.js';
import { OrdersModel } from './orders-model.js';
import { OrderCommand, CreateOrderCommandPayload, OrderEvent, tracedEachMessage } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'orders-service-worker',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'orders-service-group' });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const ordersModel = new OrdersModel(producer);

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
    .execute();
    
  await db.schema
    .createTable('order_item')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('order_id', 'varchar', (col) => col.notNull())
    .addColumn('product_id', 'varchar', (col) => col.notNull())
    .addColumn('quantity', 'integer', (col) => col.notNull())
    .execute();
    
  console.log('🛒 [Orders DB] Initialized tables');
}

async function start() {
  await initDB();
  await producer.connect();
  await consumer.connect();
  
  // Listen to BOTH commands from gateway and saga responses from other services
  await consumer.subscribe({ topic: 'orders-commands-topic', fromBeginning: true });
  await consumer.subscribe({ topic: 'orders-topic', fromBeginning: true });
  console.log('🛒 [Orders Worker] Listening for commands and saga responses');

  await consumer.run({
    eachMessage: tracedEachMessage(async ({ topic, partition, message }) => {
      if (!message.value) return;
      
      try {
        if (topic === 'orders-commands-topic') {
          const command: OrderCommand = JSON.parse(message.value.toString());
          if (command.commandType === 'CREATE_ORDER_START') {
            await ordersModel.createPendingOrder(command.payload as CreateOrderCommandPayload);
          }
        } else if (topic === 'orders-topic') {
          const event: OrderEvent = JSON.parse(message.value.toString());
          if (!event.eventType) return;
          
          if (['STOCK_RESERVED_END', 'STOCK_DENIED_END', 'CUSTOMER_VALIDATED_END', 'CUSTOMER_INVALID_END'].includes(event.eventType)) {
            console.log(`🛒 [Orders Worker] Received saga response: ${event.eventType} for ${event.orderId}`);
            await ordersModel.handleSagaResponse(event.orderId, event.eventType as any);
          }
        }
      } catch (e) {
        console.error(`🛒 Failed to process message from ${topic}`, e);
      }
    }),
  });
}

start().catch(console.error);
