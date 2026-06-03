import { Kafka, Partitioners } from 'kafkajs';
import { db } from './db.js';
import { CatalogModel } from './catalog-model.js';
import { CatalogCommand, CreateProductCommandPayload, UpdatePriceCommandPayload, tracedEachMessage, KafkaLogger } from 'shared-contracts';
import { sql } from 'kysely';
import { initScheduler, priceUpdateQueue } from './scheduler.js';
import Redis from 'ioredis';

const kafka = new Kafka({
  clientId: 'catalog-service-worker',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'catalog-service-group' });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const sysLogger = new KafkaLogger(producer, 'catalog-service');
const catalogModel = new CatalogModel(producer, sysLogger);
const redis = new Redis('redis://localhost:6379');

async function initDB() {
  await db.schema
    .createTable('product')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('product_id', 'varchar', (col) => col.notNull().unique())
    .addColumn('title', 'varchar', (col) => col.notNull())
    .addColumn('price', 'numeric', (col) => col.notNull())
    .addColumn('stock_count', 'integer', (col) => col.notNull())
    .execute();
  
  // Safely add columns for existing tables
  try {
    await db.schema.alterTable('product').addColumn('thumbnail', 'varchar', (col) => col.notNull().defaultTo('')).execute();
  } catch (e) { /* ignore if exists */ }
  
  try {
    await db.schema.alterTable('product').addColumn('image', 'varchar', (col) => col.notNull().defaultTo('')).execute();
  } catch (e) { /* ignore if exists */ }

  try {
    await db.schema.alterTable('product').addColumn('description', 'varchar').execute();
  } catch (e) { /* ignore if exists */ }
  
  try {
    await db.schema.alterTable('product').addColumn('created_at', 'timestamp', (col) => col.defaultTo(sql`now()`).notNull()).execute();
  } catch (e) { /* ignore if exists */ }

  try {
    await db.schema.alterTable('product').addColumn('updated_at', 'timestamp', (col) => col.defaultTo(sql`now()`).notNull()).execute();
  } catch (e) { /* ignore if exists */ }

  try {
    await db.schema.alterTable('product').addColumn('deleted_at', 'timestamp').execute();
  } catch (e) { /* ignore if exists */ }

  await db.schema
    .createTable('scheduled_price_update')
    .ifNotExists()
    .addColumn('id', 'serial', (col) => col.primaryKey())
    .addColumn('product_id', 'varchar', (col) => col.notNull())
    .addColumn('new_price', 'numeric', (col) => col.notNull())
    .addColumn('trigger_at', 'timestamp', (col) => col.notNull())
    .execute();

  console.log('📦 [Catalog DB] Initialized product & schedule tables');
}

async function syncScheduledUpdates() {
  const pendingSchedules = await db.selectFrom('scheduled_price_update').selectAll().execute();
  for (const schedule of pendingSchedules) {
    const delay = new Date(schedule.trigger_at).getTime() - Date.now();
    await priceUpdateQueue.add('updatePrice', {
      dbRowId: schedule.id,
      productId: schedule.product_id,
      newPrice: schedule.new_price
    }, { delay: Math.max(0, delay), jobId: `price_update_${schedule.id}` });
  }
  console.log(`📦 [Catalog Worker] Synced ${pendingSchedules.length} scheduled price updates to BullMQ.`);
}

async function start() {
  await initDB();
  await producer.connect();
  await consumer.connect();
  
  await consumer.subscribe({ topic: 'catalog-commands-topic', fromBeginning: false });
  await consumer.subscribe({ topic: 'orders-topic', fromBeginning: false });
  console.log('📦 [Catalog Worker] Listening for commands and saga orders');

  await consumer.run({
    eachMessage: tracedEachMessage(async ({ topic, message }) => {
      if (!message.value) return;
      
      if (topic === 'orders-topic') {
        const event = JSON.parse(message.value.toString());
        
        // Idempotency check
        const eventId = `${event.eventType}:${event.orderId}`;
        const alreadySeen = await redis.set(`idem:catalog:${eventId}`, '1', 'EX', 86400, 'NX');
        if (!alreadySeen) {
          console.log(`📦 [Idempotency] Skipping duplicate event: ${eventId}`);
          return;
        }

        if (event.eventType === 'ORDER_PENDING_END') {
          console.log(`📦 [Catalog Worker] Received PENDING order ${event.orderId}. Attempting stock reservation...`);
          await catalogModel.handleOrderPending(event);
        }
        return;
      }

      const command: CatalogCommand = JSON.parse(message.value.toString());
      
      console.log(`📦 [Catalog Worker] Received command: ${command.commandType}`);

      try {
        switch (command.commandType) {
          case 'CREATE_PRODUCT_START': {
            const payload = command.payload as CreateProductCommandPayload;
            await catalogModel.createProduct({
              productId: payload.productId,
              title: payload.title,
              price: payload.price,
              stockCount: payload.stockCount,
              description: (payload as any).description,
              thumbnail: payload.thumbnail || payload.image || 'http://localhost:3001/aws-mini-default.png',
              image: payload.image || payload.thumbnail || 'http://localhost:3001/aws-mini-default.png'
            });
            break;
          }
          case 'UPDATE_PRICE_START': {
            const payload = command.payload as UpdatePriceCommandPayload;
            await catalogModel.updatePrice(payload.productId, payload.newPrice);
            break;
          }
          case 'UPDATE_PRODUCT_START': {
            await catalogModel.updateProduct(command.payload);
            break;
          }
          case 'DELETE_PRODUCT_START': {
            await catalogModel.deleteProduct((command.payload as any).productId);
            break;
          }
          case 'SCHEDULE_PRICE_UPDATE_COMMAND': {
            await catalogModel.schedulePriceUpdate(command.payload);
            break;
          }
          case 'RESTORE_STOCK_COMMAND': {
            const payload = command.payload as any;
            const eventId = `RESTORE_STOCK_COMMAND:${payload.orderId}`;
            const alreadySeen = await redis.set(`idem:catalog:${eventId}`, '1', 'EX', 86400, 'NX');
            if (!alreadySeen) {
              console.log(`📦 [Idempotency] Skipping duplicate command: ${eventId}`);
              break;
            }
            await catalogModel.restoreStock(command.payload);
            break;
          }
          default:
            console.warn(`📦 Unknown command type: ${command.commandType}`);
        }
      } catch (e) {
        console.error(`📦 Failed to process command ${command.commandType}`, e);
      }
    })
  });

  initScheduler(catalogModel);
  await syncScheduledUpdates();

  const shutdown = async () => {
    console.log('📦 [Catalog Worker] Shutting down gracefully...');
    await redis.quit();
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

start().catch(console.error);
