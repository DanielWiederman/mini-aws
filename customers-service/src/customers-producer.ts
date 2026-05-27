// customers-service/src/customers-producer.ts
import { Kafka, Producer } from 'kafkajs';
import { CustomerEvent } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'customers-service',
  brokers: ['localhost:9092']
});

const producer: Producer = kafka.producer();

// Mock database of platform customers
const sampleCustomers: CustomerEvent[] = [
  { customerId: 'CUST-01', fullName: 'Alice Smith', email: 'alice@example.com', tier: 'STANDARD' },
  { customerId: 'CUST-02', fullName: 'Bob Jones', email: 'bob@example.com', tier: 'PREMIUM' }
];

async function startCustomerStream() {
  await producer.connect();
  console.log('👥 Customers Service Online. Streaming users to Kafka...');

  // 1. Broadcast existing customers immediately
  for (const customer of sampleCustomers) {
    await producer.send({
      topic: 'customer-topic',
      messages: [{
        key: customer.customerId,
        value: JSON.stringify(customer)
      }]
    });
    console.log(`[Customers] Streamed customer account: ${customer.fullName} (${customer.tier})`);
  }

  // 2. Simulate Bob upgrading his account tier to PREMIUM after 15 seconds
  setTimeout(async () => {
    const bobUpgrade: CustomerEvent = {
      customerId: 'CUST-02',
      fullName: 'Bob Jones',
      email: 'bob@example.com',
      tier: 'PREMIUM'
    };

    await producer.send({
      topic: 'customer-topic',
      messages: [{
        key: bobUpgrade.customerId,
        value: JSON.stringify(bobUpgrade)
      }]
    });
    console.log(`[Customers Update] LIVE Profile Shift! Bob Jones upgraded to PREMIUM!`);
  }, 15000);
}

startCustomerStream().catch(console.error);
