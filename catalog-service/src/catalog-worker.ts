import { Kafka } from 'kafkajs';
import { db } from './db.js';
import { CatalogModel } from './catalog-model.js';
import { CatalogCommand, CreateProductCommandPayload, UpdatePriceCommandPayload } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'catalog-service-worker',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'catalog-service-group' });
const producer = kafka.producer();
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
  console.log('📦 [Catalog DB] Initialized product table');
}

async function start() {
  await initDB();
  await producer.connect();
  await consumer.connect();
  
  await consumer.subscribe({ topic: 'catalog-commands-topic', fromBeginning: true });
  console.log('📦 [Catalog Worker] Listening for commands on catalog-commands-topic');

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
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
              stockCount: payload.stockCount
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
    }
  });
}

start().catch(console.error);
