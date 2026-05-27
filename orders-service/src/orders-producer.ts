// orders-service/src/orders-producer.ts
import { Kafka, Producer } from 'kafkajs';
import { OrderEvent } from './interfaces';

const kafka = new Kafka({
  clientId: 'orders-service',
  brokers: ['localhost:9092']
});

const producer: Producer = kafka.producer();

async function startOrdersStream() {
  await producer.connect();
  console.log('🛒 Orders Write/Command Service Online. Simulating checkout streams...');

  let orderCount = 1;

  // Simulate a live customer shopping checkout transaction event every 6 seconds
  setInterval(async () => {
    // Alternate between our two mock customers
    const currentCustomer = orderCount % 2 === 0 ? 'CUST-02' : 'CUST-01';
    const orderId = `ORD-999${orderCount}`;

    // Mock a random basket containing item links
    const orderPayload: OrderEvent = {
      orderId: orderId,
      customerId: currentCustomer,
      items: [
        { productId: 'PROD-100', quantity: Math.floor(Math.random() * 2) + 1 }, // Wireless Mouse
        { productId: 'PROD-200', quantity: Math.floor(Math.random() * 1) + 1 }  // Mechanical Keyboard
      ],
      status: 'COMPLETED',
      timestamp: new Date().toISOString()
    };

    await producer.send({
      topic: 'orders-topic',
      messages: [{
        key: orderPayload.orderId, // Order ID as message partition key
        value: JSON.stringify(orderPayload)
      }]
    });

    console.log(`[Orders Command] 🚀 Checkout Streamed: ${orderId} for Customer ${currentCustomer}`);
    orderCount++;
  }, 6000);
}

startOrdersStream().catch(console.error);
