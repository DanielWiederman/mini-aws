import { Kafka, Consumer, Producer } from 'kafkajs';
import { initDb } from './db.js';
import { CustomerModel } from './customer-model.js';
import { CustomerCommand } from 'shared-contracts';

const kafka = new Kafka({
  clientId: 'customers-service-worker',
  brokers: ['localhost:9092']
});

const producer: Producer = kafka.producer();
const consumer: Consumer = kafka.consumer({ groupId: 'customers-worker-group' });

async function startWorker() {
  console.log('👥 Customers Service Worker Online. Connecting to Kafka & Postgres...');

  await initDb();
  await producer.connect();
  await consumer.connect();

  const customerModel = new CustomerModel(producer);

  await consumer.subscribe({ topic: 'customer-commands-topic', fromBeginning: false });

  console.log('🎧 Listening for Commands on customer-commands-topic...');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return;
      
      const commandStr = message.value.toString();
      const command: CustomerCommand = JSON.parse(commandStr);

      console.log(`[Worker] Received Command: ${command.commandType}`);

      try {
        if (command.commandType === 'CREATE_CUSTOMER_COMMAND') {
          await customerModel.createCustomer(command.payload);
        } else if (command.commandType === 'UPGRADE_TIER_COMMAND') {
          await customerModel.upgradeTier(command.payload.customerId, command.payload.newTier);
        }
      } catch (err) {
        console.error(`[Worker] Error processing command ${command.commandType}`, err);
      }
    },
  });
}

startWorker().catch(console.error);
