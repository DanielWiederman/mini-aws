// catalog-service/src/catalog-producer.ts
import { Kafka, Producer } from 'kafkajs';
import { CatalogEvent } from './interfaces';

const kafka = new Kafka({
  clientId: 'catalog-service',
  brokers: ['localhost:9092']
});

const producer: Producer = kafka.producer();

// Mock database of products we are going to broadcast to the cluster
const sampleProducts: CatalogEvent[] = [
  { productId: 'PROD-100', title: 'Wireless Mouse', price: 25.00, stockCount: 50 },
  { productId: 'PROD-200', title: 'Mechanical Keyboard', price: 85.00, stockCount: 30 },
  { productId: 'PROD-300', title: '4K Monitor', price: 350.00, stockCount: 15 }
];

async function startCatalogStream() {
  await producer.connect();
  console.log('📦 Catalog Service Online. Streaming products to Kafka...');

  // 1. Broadcast the initial catalog inventory immediately
  for (const product of sampleProducts) {
    await producer.send({
      topic: 'catalog-topic',
      messages: [{
        key: product.productId,
        value: JSON.stringify(product)
      }]
    });
    console.log(`[Catalog] Streamed product definition: ${product.title} ($${product.price})`);
  }

  // 2. Simulate a live price fluctuation event every 10 seconds to show dynamic updates
  setInterval(async () => {
    // Pick a random product and slightly shift its price
    const randomProduct = sampleProducts[Math.floor(Math.random() * sampleProducts.length)];
    const priceChange = (Math.random() * 4 - 2); // change price by -$2.00 to +$2.00
    randomProduct.price = parseFloat((randomProduct.price + priceChange).toFixed(2));

    await producer.send({
      topic: 'catalog-topic',
      messages: [{
        key: randomProduct.productId,
        value: JSON.stringify(randomProduct)
      }]
    });
    console.log(`[Catalog Updates] LIVE Price Shift! ${randomProduct.title} is now $${randomProduct.price}`);
  }, 10000);
}

startCatalogStream().catch(console.error);
