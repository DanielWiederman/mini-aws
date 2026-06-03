import { Kafka, Consumer, Producer, Partitioners } from 'kafkajs';
import { initDb } from './db.js';
import { CustomerModel } from './customer-model.js';
import { CustomerCommand, tracedEachMessage, KafkaLogger, withDLQ } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'customers-service-worker',
  brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')]
});

const producer: Producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, allowAutoTopicCreation: true });
const consumer: Consumer = kafka.consumer({ groupId: 'customers-worker-group' });

async function startWorker() {
  console.log('👥 Customers Service Worker Online. Connecting to Kafka & Postgres...');

  await initDb();
  await producer.connect();
  await consumer.connect();

  const sysLogger = new KafkaLogger(producer, 'customers-service');
  const customerModel = new CustomerModel(producer, sysLogger);

  await consumer.subscribe({ topic: 'customer-commands-topic', fromBeginning: false });
  await consumer.subscribe({ topic: 'orders-topic', fromBeginning: false });

  console.log('🎧 Listening for Commands on customer-commands-topic and orders-topic...');

  const dlqRetryMap = new Map<string, number>();

  await consumer.run({
    eachMessage: tracedEachMessage(async ({ topic, partition, message }) => {
      if (!message.value) return;

      await withDLQ(producer, topic, partition, message.offset, message, async () => {
        if (topic === 'orders-topic') {
          const event = JSON.parse(message.value!.toString());
          if (event.eventType === 'ORDER_PENDING_END') {
            console.log(`👤 [Customers Worker] Received PENDING order ${event.orderId}. Validating customer...`);
            await customerModel.handleOrderPending(event);
          }
          return;
        }
        
        const commandStr = message.value!.toString();
        const command: CustomerCommand = JSON.parse(commandStr);

        console.log(`[Worker] Received Command: ${command.commandType}`);

        if (command.commandType === 'CREATE_CUSTOMER_COMMAND') {
          await customerModel.createCustomer(command.payload);
        } else if (command.commandType === 'UPGRADE_TIER_COMMAND') {
          await customerModel.upgradeTier(command.payload.customerId, command.payload.newTier);
        }
      }, dlqRetryMap);
    }),
  });

  const shutdown = async () => {
    console.log('👥 [Customers Worker] Shutting down gracefully...');
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

startWorker().catch(console.error);
