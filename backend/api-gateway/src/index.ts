import express from 'express';
import path from 'path';
import { Kafka, Partitioners } from 'kafkajs';
import { CustomerCommand, CreateCustomerCommandPayload, UpgradeTierCommandPayload, CatalogCommand, CreateProductCommandPayload, UpdatePriceCommandPayload, OrderCommand, CreateOrderCommandPayload, sendTraced, KafkaLogger } from 'shared-contracts';
import { open } from 'lmdb';
import Redis from 'ioredis';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import cookieParser from 'cookie-parser';
import cors from 'cors';

const app = express();
app.use(express.json());
app.use(cookieParser());
app.use(cors({
  origin: true,
  credentials: true
}));

const idempotencyDb = open({ path: './db/idempotency', compression: true });

// CQRS Query Side: Connect to the highly-available Redis store
const redis = new Redis('redis://localhost:6379');

// --- TOKEN BUCKET RATE LIMITER ---
const tokenBucketScript = `
  local capacity = tonumber(ARGV[1])
  local refillRate = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local requested = 1

  local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
  local tokens = tonumber(bucket[1])
  local last_refill = tonumber(bucket[2])

  if not tokens then
    tokens = capacity
    last_refill = now
  else
    local elapsed = math.max(0, now - last_refill)
    local refilled = elapsed * refillRate
    tokens = math.min(capacity, tokens + refilled)
    last_refill = now
  end

  if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', last_refill)
    redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refillRate))
    return 1
  else
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'last_refill', last_refill)
    redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refillRate))
    return 0
  end
`;

async function checkRateLimit(key: string, capacity: number, refillRatePerSec: number): Promise<boolean> {
  const now = Date.now() / 1000;
  const result = await redis.eval(tokenBucketScript, 1, key, capacity, refillRatePerSec, now);
  return result === 1;
}
// ---------------------------------

const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const sysLogger = new KafkaLogger(producer, 'api-gateway');

const idempotencyMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (req.method !== 'POST' || req.path === '/api/customers/login' || req.path === '/api/customers/logout') return next();
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
    
    if (!payload.customerId || !payload.firstName || !payload.lastName || !payload.email || !payload.password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const command: CustomerCommand = {
      commandType: 'CREATE_CUSTOMER_COMMAND',
      payload
    };

    await sendTraced(producer, 'customer-commands-topic', [{
      key: payload.customerId,
      value: JSON.stringify(command)
    }]);

    sysLogger.info(`Accepted customer creation command for ${payload.customerId}`, payload).catch(() => {});
    const responseData = { message: 'Customer creation accepted', customerId: payload.customerId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/customers/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Missing email or password' });
    }

    // --- Redis Token Bucket Rate Limiting ---
    const ip = req.header('X-Test-IP') || req.ip || req.socket.remoteAddress || 'unknown';
    const rateLimitKey = `ratelimit:login:${ip}`;
    
    // Capacity 5, Refill Rate: 0.5 tokens/sec (1 token every 2 seconds)
    const allowed = await checkRateLimit(rateLimitKey, 5, 0.5);
    if (!allowed) {
      return res.status(429).json({ error: 'Too many login attempts from this IP. Please try again later.' });
    }
    // ----------------------------------------

    const authStr = await redis.hget('auth_view', email);
    if (!authStr) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const authData = JSON.parse(authStr);
    const isValid = await bcrypt.compare(password, authData.passwordHash);
    
    if (!isValid) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = jwt.sign({ customerId: authData.customerId, role: authData.role || 'CUSTOMER' }, process.env.JWT_SECRET || 'secret_key', { expiresIn: '1h' });
    res.cookie('jwt', token, { httpOnly: true, maxAge: 3600000 });
    res.json({ token, customerId: authData.customerId, role: authData.role || 'CUSTOMER' });

  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/customers/logout', (req, res) => {
  res.clearCookie('jwt');
  res.json({ message: 'Logged out successfully' });
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

    await sendTraced(producer, 'customer-commands-topic', [{
      key: customerId,
      value: JSON.stringify(command)
    }]);

    console.log(`[API Gateway] Published UPGRADE_TIER_COMMAND for ${customerId}`);
    const responseData = { message: 'Tier upgrade accepted', customerId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// --- RBAC MIDDLEWARES ---
const requireAdmin = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  const token = req.cookies.jwt || req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret_key') as any;
    if (decoded.role !== 'ADMIN' && decoded.role !== 'SUPER_ADMIN') {
      return res.status(403).json({ error: 'Forbidden' });
    }
    (req as any).user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid token' });
  }
};

const requireSuperAdmin = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  const token = req.cookies.jwt || req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret_key') as any;
    if (decoded.role !== 'SUPER_ADMIN') {
      return res.status(403).json({ error: 'Forbidden: SUPER_ADMIN required' });
    }
    (req as any).user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid token' });
  }
};

// --- CUSTOMERS COMMAND SIDE ---
app.post('/api/admins', requireSuperAdmin, async (req, res) => {
  try {
    const payload: CreateCustomerCommandPayload = req.body;
    if (!payload.customerId || !payload.firstName || !payload.lastName || !payload.email || !payload.password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    payload.role = 'ADMIN';

    const command: CustomerCommand = { commandType: 'CREATE_CUSTOMER_COMMAND', payload };
    await sendTraced(producer, 'customer-commands-topic', [{ key: payload.customerId, value: JSON.stringify(command) }]);

    const responseData = { message: 'Admin creation accepted', customerId: payload.customerId };
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
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

// --- CATALOG COMMAND SIDE (WRITE) ---

app.post('/api/catalog', async (req, res) => {
  try {
    const payload: CreateProductCommandPayload = req.body;
    
    if (!payload.productId || !payload.title || typeof payload.price !== 'number' || typeof payload.stockCount !== 'number') {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    
    // Explicitly pick allowed fields
    const sanitizedPayload: CreateProductCommandPayload = {
      productId: payload.productId,
      title: payload.title,
      price: payload.price,
      stockCount: payload.stockCount,
      description: payload.description,
      thumbnail: payload.thumbnail,
      image: payload.image
    };

    const command: CatalogCommand = {
      commandType: 'CREATE_PRODUCT_START',
      payload: sanitizedPayload
    };

    await sendTraced(producer, 'catalog-commands-topic', [
      { key: payload.productId, value: JSON.stringify(command) }
    ]);

    sysLogger.info(`Accepted product creation command for ${payload.productId}`, payload).catch(() => {});
    console.log(`[API Gateway] Published CREATE_PRODUCT_START for ${payload.productId}`);
    const responseData = { message: 'Product creation accepted', productId: payload.productId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/catalog/:id/price', async (req, res) => {
  try {
    const productId = req.params.id;
    const { price } = req.body;

    if (typeof price !== 'number') {
      return res.status(400).json({ error: 'Invalid price' });
    }

    const payload: UpdatePriceCommandPayload = {
      productId,
      newPrice: price
    };

    const command: CatalogCommand = {
      commandType: 'UPDATE_PRICE_START',
      payload
    };

    await sendTraced(producer, 'catalog-commands-topic', [
      { key: productId, value: JSON.stringify(command) }
    ]);

    sysLogger.info(`Accepted scheduled price update command for ${productId}`, { productId, newPrice: price }).catch(() => {});
    console.log(`[API Gateway] Published UPDATE_PRICE_START for ${productId}`);
    
    const responseData = { message: 'Price update accepted', productId };
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.put('/api/catalog/:id', requireAdmin, async (req, res) => {
  try {
    const productId = req.params.id;
    const payload = req.body;
    payload.productId = productId;
    
    const command: CatalogCommand = { commandType: 'UPDATE_PRODUCT_START', payload };
    await sendTraced(producer, 'catalog-commands-topic', [{ key: productId, value: JSON.stringify(command) }]);

    const responseData = { message: 'Product update accepted', productId };
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.delete('/api/catalog/:id', requireAdmin, async (req, res) => {
  try {
    const productId = req.params.id;
    const command: CatalogCommand = { commandType: 'DELETE_PRODUCT_START', payload: { productId } as any };
    await sendTraced(producer, 'catalog-commands-topic', [{ key: productId, value: JSON.stringify(command) }]);

    const responseData = { message: 'Product deletion accepted', productId };
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/catalog/:id/price-schedule', requireAdmin, async (req, res) => {
  try {
    const productId = req.params.id;
    const { newPrice, triggerAt } = req.body;
    
    if (typeof newPrice !== 'number' || !triggerAt) {
      return res.status(400).json({ error: 'Invalid price or triggerAt' });
    }

    const command: CatalogCommand = { 
      commandType: 'SCHEDULE_PRICE_UPDATE_COMMAND', 
      payload: { productId, newPrice, triggerAt } as any 
    };
    
    await sendTraced(producer, 'catalog-commands-topic', [{ key: productId, value: JSON.stringify(command) }]);

    const responseData = { message: 'Price schedule accepted', productId };
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    res.status(500).json({ error: 'Internal Server Error' });
  }
});
app.get('/api/customers/me', async (req, res) => {
  try {
    const token = req.cookies.jwt || req.headers.authorization?.split(' ')[1];
    if (!token) {
      return res.status(401).json({ error: 'Unauthorized: No token provided' });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret_key') as { customerId: string };
    const customerStr = await redis.hget('customers_view', decoded.customerId);
    
    if (!customerStr) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    
    res.json(JSON.parse(customerStr));
  } catch (err) {
    console.error(err);
    res.status(401).json({ error: 'Unauthorized: Invalid token' });
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

// --- CATALOG READ SIDE (QUERIES) ---

app.get('/api/catalog', async (req, res) => {
  try {
    let isAdmin = false;
    const token = req.cookies.jwt || req.headers.authorization?.split(' ')[1];
    if (token) {
      try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret_key') as any;
        if (decoded.role === 'ADMIN' || decoded.role === 'SUPER_ADMIN') {
          isAdmin = true;
        }
      } catch (e) {
        // Ignore invalid token for rate limiting bypass purposes
      }
    }

    if (!isAdmin) {
      // --- Redis Token Bucket Rate Limiting for Public Search ---
      const ip = req.header('X-Test-IP') || req.ip || req.socket.remoteAddress || 'unknown';
      const rateLimitKey = `ratelimit:search:${ip}`;
      
      // Capacity 10, Refill Rate: 1 token/sec
      const allowed = await checkRateLimit(rateLimitKey, 10, 1);
      if (!allowed) {
        return res.status(429).json({ error: 'Too many search requests from this IP. Please try again later.' });
      }
      // ----------------------------------------
    }

    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const q = req.query.q as string;
    const sort = req.query.sort as string;
    const startIndex = (page - 1) * limit;

    let queryStr = '*';
    if (q && q.trim().length > 0) {
      // Prefix matching instead of leading wildcard to utilize the inverted index
      const terms = q.trim().split(/\s+/).map(t => `${t}*`).join(' ');
      queryStr = `@title:(${terms})`;
    }

    const searchArgs: any[] = [
      'FT.SEARCH', 'idx:catalog', queryStr,
      'LIMIT', startIndex, limit
    ];

    if (sort === 'price_desc') {
      searchArgs.push('SORTBY', 'price', 'DESC');
    } else if (sort === 'price_asc' || (!q || q.trim().length === 0)) {
      searchArgs.push('SORTBY', 'price', 'ASC');
    }

    // Run RediSearch query
    const searchRes = await redis.call(...(searchArgs as [string, ...string[]])) as any[];

    // searchRes format: [total_results, key1, [key1_fields...], key2, [key2_fields...], ...]
    const total = searchRes[0] as number;
    const results = [];
    
    for (let i = 1; i < searchRes.length; i += 2) {
      const fields = searchRes[i+1] as string[];
      // fields array is typically ['$', '{"productId":"...","title":"..."}']
      const dollarIndex = fields.indexOf('$');
      if (dollarIndex !== -1 && fields[dollarIndex + 1]) {
        results.push(JSON.parse(fields[dollarIndex + 1]));
      } else if (fields.length > 0 && fields[0].startsWith('{')) {
        results.push(JSON.parse(fields[0])); // Fallback depending on driver behavior
      }
    }

    res.json({
      data: results,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to search catalog from Redis' });
  }
});

app.get('/api/catalog/:id', async (req, res) => {
  try {
    const productStr = await redis.call('JSON.GET', `catalog:${req.params.id}`) as string | null;
    if (!productStr) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json(JSON.parse(productStr));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read product from Redis JSON' });
  }
});

// --- ORDERS COMMAND SIDE (WRITE) ---

app.post('/api/orders', async (req, res) => {
  try {
    const payload: CreateOrderCommandPayload = req.body;
    
    if (!payload.orderId || !payload.customerId || !payload.items || !Array.isArray(payload.items)) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // --- Redis Token Bucket Rate Limiting (by Customer ID) ---
    const rateLimitKey = `ratelimit:orders:${payload.customerId}`;
    // Capacity 5, Refill Rate: 0.5 tokens/sec
    const allowed = await checkRateLimit(rateLimitKey, 5, 0.5);
    if (!allowed) {
      return res.status(429).json({ error: 'Too many orders placed recently. Please try again later.' });
    }
    // ---------------------------------------------------------

    const command: OrderCommand = {
      commandType: 'CREATE_ORDER_START',
      payload
    };

    await sendTraced(producer, 'orders-commands-topic', [
      { key: payload.orderId, value: JSON.stringify(command) }
    ]);

    sysLogger.info(`Accepted order creation command for ${payload.orderId} (Customer: ${payload.customerId})`, payload).catch(() => {});
    console.log(`[API Gateway] Published CREATE_ORDER_START for ${payload.orderId}`);
    const responseData = { message: 'Order creation accepted', orderId: payload.orderId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    sysLogger.error('API Gateway Error', err).catch(() => {});
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// --- ORDERS READ SIDE (QUERIES) ---
app.get('/api/orders/:id', async (req, res) => {
  try {
    const orderStr = await redis.call('JSON.GET', `order:${req.params.id}`) as string | null;
    if (!orderStr) {
      return res.status(404).json({ error: 'Order not found' });
    }
    res.json(JSON.parse(orderStr));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read order from Redis' });
  }
});

app.get('/api/orders', requireAdmin, async (req, res) => {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;
    const offset = (page - 1) * limit;
    const q = (req.query.q as string || '').trim();
    const startDate = req.query.startDate as string;
    const endDate = req.query.endDate as string;
    const status = req.query.status as string;

    const queryParts = [];

    // 1. Unified Search text filter
    if (q) {
      const tokens = q.replace(/[^a-zA-Z0-9 _-]/g, '').split(/\s+/).filter(Boolean);
      if (tokens.length > 0) {
        const tokenQueries = tokens.map(t => `((@orderId:*${t}*) | (@customerName:*${t}*) | (@itemTitle:*${t}*))`);
        queryParts.push(tokenQueries.join(' '));
      }
    }
    
    // 2. Date boundaries filter
    if (startDate || endDate) {
      const startMs = startDate ? new Date(startDate).getTime() : 0;
      let endMs: number | string = '+inf';
      if (endDate) {
        const endD = new Date(endDate);
        endD.setHours(23, 59, 59, 999);
        endMs = endD.getTime();
      }
      queryParts.push(`@createdAt:[${startMs} ${endMs}]`);
    }

    // 3. Status filter
    if (status) {
      queryParts.push(`@status:{${status}}`);
    }

    const ftQuery = queryParts.length > 0 ? queryParts.join(' ') : '*';

    const searchArgs: any[] = [
      'FT.SEARCH', 'idx:orders', ftQuery,
      'SORTBY', 'createdAt', 'DESC',
      'LIMIT', offset.toString(), limit.toString()
    ];

    const searchRes = await redis.call(...(searchArgs as [string, ...string[]])) as any[];
    
    const totalCount = searchRes[0] as number;
    const data = [];
    
    for (let i = 1; i < searchRes.length; i += 2) {
      const fields = searchRes[i+1] as string[];
      const dollarIndex = fields.indexOf('$');
      if (dollarIndex !== -1 && fields[dollarIndex + 1]) {
        data.push(JSON.parse(fields[dollarIndex + 1]));
      } else if (fields.length > 0 && fields[0].startsWith('{')) {
        data.push(JSON.parse(fields[0]));
      }
    }

    res.json({
      data,
      total: totalCount,
      page,
      limit,
      totalPages: Math.ceil(totalCount / limit)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch orders' });
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
