import express from 'express';
import path from 'path';
import { Kafka } from 'kafkajs';
import { CustomerCommand, CreateCustomerCommandPayload, UpgradeTierCommandPayload } from 'shared-contracts';
import { open } from 'lmdb';
import Redis from 'ioredis';

const app = express();
app.use(express.json());

const idempotencyDb = open({ path: './db/idempotency', compression: true });

// CQRS Query Side: Connect to the highly-available Redis store
const redis = new Redis('redis://localhost:6379');

const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const idempotencyMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (req.method !== 'POST') return next();
  const key = req.header('Idempotency-Key');
  if (!key) {
    return res.status(400).json({ error: 'Missing Idempotency-Key header' });
  }
  const cachedResponse = idempotencyDb.get(key);
  if (cachedResponse) {
    console.log(`[API Gateway] Idempotency cache hit for key ${key}`);
    return res.status(202).json(cachedResponse);
  }
  next();
};

app.use(idempotencyMiddleware);

app.post('/api/customers', async (req, res) => {
  try {
    const payload: CreateCustomerCommandPayload = req.body;
    
    if (!payload.customerId || !payload.firstName || !payload.lastName || !payload.email) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const command: CustomerCommand = {
      commandType: 'CREATE_CUSTOMER_COMMAND',
      payload
    };

    await producer.send({
      topic: 'customer-commands-topic',
      messages: [{
        key: payload.customerId,
        value: JSON.stringify(command)
      }]
    });

    console.log(`[API Gateway] Published CREATE_CUSTOMER_COMMAND for ${payload.customerId}`);
    const responseData = { message: 'Customer creation accepted', customerId: payload.customerId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/customers/:id/tier', async (req, res) => {
  try {
    const customerId = req.params.id;
    const { tier } = req.body;

    if (!tier || (tier !== 'STANDARD' && tier !== 'PREMIUM')) {
      return res.status(400).json({ error: 'Invalid tier' });
    }

    const payload: UpgradeTierCommandPayload = {
      customerId,
      newTier: tier
    };

    const command: CustomerCommand = {
      commandType: 'UPGRADE_TIER_COMMAND',
      payload
    };

    await producer.send({
      topic: 'customer-commands-topic',
      messages: [{
        key: customerId,
        value: JSON.stringify(command)
      }]
    });

    console.log(`[API Gateway] Published UPGRADE_TIER_COMMAND for ${customerId}`);
    const responseData = { message: 'Tier upgrade accepted', customerId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// --- CQRS READ SIDE (QUERIES) ---

app.get('/api/customers', async (req, res) => {
  try {
    const tierFilter = req.query.tier as string;
    
    const allCustomers = await redis.hgetall('customers_view');
    const results = [];
    
    for (const [key, valueStr] of Object.entries(allCustomers)) {
      const customer = JSON.parse(valueStr);
      if (tierFilter && customer.tier !== tierFilter) continue;
      results.push(customer);
    }
    
    res.json(results);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read from materialized view in Redis' });
  }
});

app.get('/api/customers/:id', async (req, res) => {
  try {
    const customerStr = await redis.hget('customers_view', req.params.id);
    if (!customerStr) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    res.json(JSON.parse(customerStr));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read from materialized view in Redis' });
  }
});

async function start() {
  await producer.connect();
  console.log('[API Gateway] Connected to Kafka');
  
  app.listen(3000, () => {
    console.log('🌐 API Gateway running on http://localhost:3000');
  });
}

start().catch(console.error);
