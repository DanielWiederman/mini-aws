import express from 'express';
import path from 'path';
import { Kafka, Partitioners } from 'kafkajs';
import { CustomerCommand, CreateCustomerCommandPayload, UpgradeTierCommandPayload, CatalogCommand, CreateProductCommandPayload, UpdatePriceCommandPayload, OrderCommand, CreateOrderCommandPayload, sendTraced } from 'shared-contracts';
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

const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

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

app.post('/api/customers/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Missing email or password' });
    }

    // --- Redis Fixed Window Rate Limiting ---
    const ip = req.ip || req.socket.remoteAddress || 'unknown';
    const rateLimitKey = `ratelimit:login:${ip}`;
    
    const attempts = await redis.incr(rateLimitKey);
    if (attempts === 1) {
      // 10 second window
      await redis.expire(rateLimitKey, 10);
    }

    if (attempts > 5) {
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

    const token = jwt.sign({ customerId: authData.customerId, role: authData.role || 'CUSTOMER' }, 'secret_key', { expiresIn: '1h' });
    res.cookie('jwt', token, { httpOnly: true, maxAge: 3600000 });
    res.json({ token, customerId: authData.customerId, role: authData.role || 'CUSTOMER' });

  } catch (err) {
    console.error(err);
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
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// --- RBAC MIDDLEWARES ---
const requireAdmin = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  const token = req.cookies.jwt || req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const decoded = jwt.verify(token, 'secret_key') as any;
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
    const decoded = jwt.verify(token, 'secret_key') as any;
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

    console.log(`[API Gateway] Published CREATE_PRODUCT_START for ${payload.productId}`);
    const responseData = { message: 'Product creation accepted', productId: payload.productId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
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

    console.log(`[API Gateway] Published UPDATE_PRICE_START for ${productId}`);
    const responseData = { message: 'Price update accepted', productId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
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

    const decoded = jwt.verify(token, 'secret_key') as { customerId: string };
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
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const allProducts = await redis.hgetall('catalog_view');
    const results = Object.values(allProducts)
      .map(v => JSON.parse(v))
      .filter(p => !p.isDeleted); // Exclude soft-deleted products
    
    // Sort by productId for deterministic pagination
    results.sort((a, b) => a.productId.localeCompare(b.productId));

    const startIndex = (page - 1) * limit;
    const endIndex = page * limit;
    const paginatedResults = results.slice(startIndex, endIndex);

    res.json({
      data: paginatedResults,
      total: results.length,
      page,
      limit,
      totalPages: Math.ceil(results.length / limit)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read catalog from Redis' });
  }
});

app.get('/api/catalog/:id', async (req, res) => {
  try {
    const productStr = await redis.hget('catalog_view', req.params.id);
    if (!productStr) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json(JSON.parse(productStr));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to read product from Redis' });
  }
});

// --- ORDERS COMMAND SIDE (WRITE) ---

app.post('/api/orders', async (req, res) => {
  try {
    const payload: CreateOrderCommandPayload = req.body;
    
    if (!payload.orderId || !payload.customerId || !payload.items || !Array.isArray(payload.items)) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // --- Redis Fixed Window Rate Limiting (by Customer ID) ---
    const rateLimitKey = `ratelimit:orders:${payload.customerId}`;
    const attempts = await redis.incr(rateLimitKey);
    if (attempts === 1) {
      await redis.expire(rateLimitKey, 10); // 10 second window
    }
    if (attempts > 5) {
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

    console.log(`[API Gateway] Published CREATE_ORDER_START for ${payload.orderId}`);
    const responseData = { message: 'Order creation accepted', orderId: payload.orderId };
    
    const idempotencyKey = req.header('Idempotency-Key') as string;
    await idempotencyDb.put(idempotencyKey, responseData);
    
    res.status(202).json(responseData);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// --- ORDERS READ SIDE (QUERIES) ---
app.get('/api/orders/:id', async (req, res) => {
  try {
    const orderStr = await redis.hget('orders_view', req.params.id);
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
    const ordersMap = await redis.hgetall('orders_view');
    const orders = Object.values(ordersMap).map(o => JSON.parse(o));
    
    // Sort by timestamp extracted from orderId (e.g. order_168038... or test_order_168038...)
    orders.sort((a, b) => {
      const matchA = a.orderId.match(/\d{13}/);
      const matchB = b.orderId.match(/\d{13}/);
      const tsA = matchA ? parseInt(matchA[0], 10) : 0;
      const tsB = matchB ? parseInt(matchB[0], 10) : 0;
      return tsB - tsA;
    });
    
    const cursor = req.query.cursor as string;
    const limit = parseInt(req.query.limit as string) || 20;

    let sortedOrders = orders;
    if (cursor) {
      const cursorIndex = sortedOrders.findIndex(o => o.orderId === cursor);
      if (cursorIndex >= 0) {
        sortedOrders = sortedOrders.slice(cursorIndex + 1);
      }
    }
    
    const paginated = sortedOrders.slice(0, limit);
    const nextCursor = paginated.length === limit ? paginated[paginated.length - 1].orderId : null;
    
    res.json({ data: paginated, nextCursor });
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
