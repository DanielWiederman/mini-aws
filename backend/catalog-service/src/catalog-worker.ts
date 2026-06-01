import { Kafka, Partitioners } from 'kafkajs';
import { db } from './db.js';
import { CatalogModel } from './catalog-model.js';
import { CatalogCommand, CreateProductCommandPayload, UpdatePriceCommandPayload, tracedEachMessage } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'catalog-service-worker',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'catalog-service-group' });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const catalogModel = new CatalogModel(producer);

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

  console.log('📦 [Catalog DB] Initialized product table');
}

async function start() {
  await initDB();
  await producer.connect();
  await consumer.connect();
  
  await consumer.subscribe({ topic: 'catalog-commands-topic', fromBeginning: true });
  await consumer.subscribe({ topic: 'orders-topic', fromBeginning: true });
  console.log('📦 [Catalog Worker] Listening for commands and saga orders');

  await consumer.run({
    eachMessage: tracedEachMessage(async ({ topic, message }) => {
      if (!message.value) return;
      
      if (topic === 'orders-topic') {
        const event = JSON.parse(message.value.toString());
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
              thumbnail: payload.thumbnail || 'http://localhost:3001/aws-mini-default.png',
              image: payload.image || 'http://localhost:3001/aws-mini-default.png'
            });
            break;
          }
          case 'UPDATE_PRICE_START': {
            const payload = command.payload as UpdatePriceCommandPayload;
            await catalogModel.updatePrice(payload.productId, payload.newPrice);
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
}

start().catch(console.error);
