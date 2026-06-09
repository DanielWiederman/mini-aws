import { Kafka, Consumer } from 'kafkajs';
import { CustomerEvent, CatalogEvent, OrderEvent, KafkaLogger } from 'shared-contracts';
import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const kafka = new Kafka({
  clientId: 'derived-view-service',
  brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')]
});

// A single consumer group that subscribes to multiple topics
const consumer: Consumer = kafka.consumer({ groupId: 'cqrs-view-group-2' });
const producer = kafka.producer();
const sysLogger = new KafkaLogger(producer, 'derived-view-service');

// --- LOCAL STATE STORES (Backed by Redis for persistence and high availability) ---
// customerTable and catalogTable removed from LMDB

let customersCount = 0;
let catalogCount = 0;

// --- THE DERIVED READ MODEL (CQRS Materialized View) ---
interface ReadModelOrderSummary {
  orderId: string;
  status?: string;
  customerName: string;
  customerTier: string;
  purchasedItems: { productId: string; title: string; qty: number; totalCost: number }[];
  invoiceTotal: number;
  processedAt: string;
  createdAt: number;
}
const finalizedOrdersView: ReadModelOrderSummary[] = [];

async function startCqrsEngine() {
  await producer.connect();
  await consumer.connect();
  
  // Subscribe to all 3 upstream data feeds
  await consumer.subscribe({ topics: ['customer-topic', 'catalog-topic', 'orders-topic'], fromBeginning: true });
  console.log('📊 CQRS Derived View Engine Online. Waiting to process streams...');

  // --- REDISEARCH INITIALIZATION ---
  const createIndex = async () => {
    await redis.call(
      'FT.CREATE', 'idx:catalog',
      'ON', 'JSON',
      'PREFIX', '1', 'catalog:',
      'SCHEMA',
      '$.title', 'AS', 'title', 'TEXT',
      '$.price', 'AS', 'price', 'NUMERIC', 'SORTABLE',
      '$.stockCount', 'AS', 'stock', 'NUMERIC', 'SORTABLE'
    );
  };

  try {
    await createIndex();
    console.log('📚 RediSearch index idx:catalog created');
  } catch (e: any) {
    if (e.message.includes('Index already exists')) {
      console.log('📚 RediSearch index idx:catalog already exists, skipping creation.');
    } else {
      console.error('Failed to create RediSearch index:', e);
    }
  }

  const createOrdersIndex = async () => {
    await redis.call(
      'FT.CREATE', 'idx:orders',
      'ON', 'JSON',
      'PREFIX', '1', 'order:',
      'SCHEMA',
      '$.orderId', 'AS', 'orderId', 'TEXT',
      '$.customerId', 'AS', 'customerId', 'TAG',
      '$.customerName', 'AS', 'customerName', 'TEXT',
      '$.status', 'AS', 'status', 'TAG',
      '$.createdAt', 'AS', 'createdAt', 'NUMERIC', 'SORTABLE',
      '$.purchasedItems[*].title', 'AS', 'itemTitle', 'TEXT',
      '$.purchasedItems[*].productId', 'AS', 'productId', 'TAG'
    );
  };
  try {
    await createOrdersIndex();
    console.log('📚 RediSearch index idx:orders created');
  } catch (e: any) {
    if (e.message.includes('Index already exists')) {
      console.log('📚 RediSearch index idx:orders already exists, skipping creation.');
    } else {
      console.error('Failed to create RediSearch index idx:orders:', e);
    }
  }
  // ---------------------------------

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const rawData = JSON.parse(message.value.toString());

      // ROUTE 1: Maintain the Customer local state table
      if (topic === 'customer-topic') {
        const customer = rawData as CustomerEvent;
        // Only process completed domain events
        if (!customer.eventType?.endsWith('_END')) return;

        const existingCustomer = await redis.hget('lmdb:customers', customer.customerId);
        const isNew = existingCustomer === null;
        await redis.hset('lmdb:customers', customer.customerId, JSON.stringify(customer));
        sysLogger.info(`Customer state materialized: ${customer.customerId} (Event: ${customer.eventType})`).catch(() => {});
        
        // Expose public profile (without password hash)
        const publicProfile = {
          customerId: customer.customerId,
          firstName: customer.firstName,
          lastName: customer.lastName,
          email: customer.email,
          tier: customer.tier,
          role: customer.role || 'CUSTOMER'
        };
        await redis.hset('customers_view', customer.customerId, JSON.stringify(publicProfile));

        // Store auth details securely
        if (customer.passwordHash) {
          await redis.hset('auth_view', customer.email, JSON.stringify({
            customerId: customer.customerId,
            passwordHash: customer.passwordHash,
            role: customer.role || 'CUSTOMER'
          }));
        }
        
        updateDashboard(isNew ? `New Customer: ${customer.firstName}` : `Tier Upgraded: ${customer.firstName} -> ${customer.tier}`);
        await patchStaleOrders('customer', customer);
      }

      // ROUTE 2: Maintain the Catalog local state table (Live price tracking)
      else if (topic === 'catalog-topic') {
        const catalog = rawData as CatalogEvent;
        if (!catalog.eventType?.endsWith('_END')) return;

        await redis.hset('lmdb:catalog', catalog.productId, JSON.stringify(catalog));
        sysLogger.info(`Catalog item materialized: ${catalog.title} (${catalog.productId})`).catch(() => {});
        
        // Phase 2: Sync Materialized View to Redis
        if (catalog.isDeleted) {
          await redis.call('JSON.DEL', `catalog:${catalog.productId}`);
          await redis.publish('catalog_pubsub', JSON.stringify(catalog));
          updateDashboard(`Catalog product deleted: ${catalog.title}`);
        } else {
          await redis.call('JSON.SET', `catalog:${catalog.productId}`, '$', JSON.stringify(catalog));
          await redis.publish('catalog_pubsub', JSON.stringify(catalog));
          updateDashboard(`Catalog updated: ${catalog.title} ($${catalog.price})`);
          await patchStaleOrders('catalog', catalog);
        }
      }

      // ROUTE 3: The Order arrives! Execute real-time stream processing join logic
      else if (topic === 'orders-topic') {
        const rawOrder = rawData as OrderEvent;
        if (!rawOrder.eventType?.endsWith('_END')) return;
        if (['STOCK_RESERVED_END', 'STOCK_DENIED_END', 'CUSTOMER_VALIDATED_END', 'CUSTOMER_INVALID_END'].includes(rawOrder.eventType)) return;
        
        // Idempotency check
        const eventId = `${rawOrder.eventType}:${rawOrder.orderId}`;
        const alreadySeen = await redis.set(`idem:cqrs:${eventId}`, '1', 'EX', 86400, 'NX');
        if (!alreadySeen) {
          console.log(`📊 [Idempotency] Skipping duplicate event: ${eventId}`);
          return;
        }
        
        // 1. Fetch details from Redis asynchronously
        const customerProfileStr = rawOrder.customerId ? await redis.hget('lmdb:customers', rawOrder.customerId) : null;
        const customerProfile = customerProfileStr ? JSON.parse(customerProfileStr) as CustomerEvent : undefined;
        const customerName = customerProfile ? `${customerProfile.firstName} ${customerProfile.lastName}` : 'Unknown Customer';
        const customerTier = customerProfile ? customerProfile.tier : 'STANDARD';

        let invoiceTotal = 0;
        const purchasedItems = [];
        
        for (const item of (rawOrder.items || [])) {
          const productDetailStr = await redis.hget('lmdb:catalog', item.productId);
          const productDetail = productDetailStr ? JSON.parse(productDetailStr) as CatalogEvent : undefined;
          const productTitle = productDetail ? productDetail.title : 'Missing Product Name';
          const currentPrice = productDetail ? Number(productDetail.price) : 0;
          const itemCost = currentPrice * item.quantity;
          
          invoiceTotal += itemCost;

          purchasedItems.push({
            productId: item.productId,
            title: productTitle,
            qty: item.quantity,
            totalCost: parseFloat(itemCost.toFixed(2))
          });
        }

        // 2. Apply business rules dynamically based on our joined state
        if (customerTier === 'PREMIUM') {
          invoiceTotal = invoiceTotal * 0.9;
        }

        const materializedOrder = {
          orderId: rawOrder.orderId,
          customerId: rawOrder.customerId,
          status: rawOrder.status,
          customerName,
          customerTier,
          purchasedItems,
          invoiceTotal: parseFloat(invoiceTotal.toFixed(2)),
          processedAt: new Date().toLocaleTimeString(),
          createdAt: rawOrder.timestamp ? new Date(rawOrder.timestamp).getTime() : Date.now()
        };

        // 3. Save directly to the materialized view in Redis and Publish to Pub/Sub
        const payloadStr = JSON.stringify(materializedOrder);
        await redis.hset('orders_view', rawOrder.orderId, payloadStr);
        await redis.call('JSON.SET', `order:${rawOrder.orderId}`, '$', payloadStr);
        await redis.publish('orders_pubsub', payloadStr);

        finalizedOrdersView.push(materializedOrder);
        updateDashboard(`Order ${rawOrder.orderId} is now ${rawOrder.status}`);
      }
    }
  });
}

async function patchStaleOrders(type: 'customer' | 'catalog', data: any) {
  if (type === 'customer') {
    const customer = data as CustomerEvent;
    while (true) {
      // Use exact match on customerId to only find orders for this customer
      const searchRes = await redis.call('FT.SEARCH', 'idx:orders', `@customerId:{${customer.customerId}} @customerName:"Unknown Customer"`, 'LIMIT', '0', '100') as any[];
      const count = searchRes[0] as number;
      if (count === 0) break;

      for (let i = 1; i < searchRes.length; i += 2) {
        const fields = searchRes[i+1] as string[];
        let orderStr = null;
        
        const dollarIndex = fields.indexOf('$');
        if (dollarIndex !== -1 && fields[dollarIndex + 1]) {
          orderStr = fields[dollarIndex + 1];
        } else if (fields.length > 0 && fields[0].startsWith('{')) {
          orderStr = fields[0];
        }
        
        if (!orderStr) continue;
        
        const order = JSON.parse(orderStr);
        // Safety check
        if (order.customerId && order.customerId !== customer.customerId) continue;

        order.customerName = `${customer.firstName} ${customer.lastName}`;
        order.customerTier = customer.tier;
        
        let invoiceTotal = 0;
        for (const item of order.purchasedItems) {
          invoiceTotal += item.totalCost;
        }
        if (order.customerTier === 'PREMIUM') {
          invoiceTotal = invoiceTotal * 0.9;
        }
        order.invoiceTotal = parseFloat(invoiceTotal.toFixed(2));
        
        const payloadStr = JSON.stringify(order);
        await redis.hset('orders_view', order.orderId, payloadStr);
        await redis.call('JSON.SET', `order:${order.orderId}`, '$', payloadStr);
        await redis.publish('orders_pubsub', payloadStr);
        console.log(`📊 [Self-Heal] Patched stale customer for order ${order.orderId}`);
      }
    }
  } else if (type === 'catalog') {
    const catalog = data as CatalogEvent;
    while (true) {
      // Use exact match on productId to only find orders containing this product
      const searchRes = await redis.call('FT.SEARCH', 'idx:orders', `@productId:{${catalog.productId}} @itemTitle:"Missing Product Name"`, 'LIMIT', '0', '100') as any[];
      const count = searchRes[0] as number;
      if (count === 0) break;

      for (let i = 1; i < searchRes.length; i += 2) {
        const fields = searchRes[i+1] as string[];
        let orderStr = null;
        
        const dollarIndex = fields.indexOf('$');
        if (dollarIndex !== -1 && fields[dollarIndex + 1]) {
          orderStr = fields[dollarIndex + 1];
        } else if (fields.length > 0 && fields[0].startsWith('{')) {
          orderStr = fields[0];
        }
        
        if (!orderStr) continue;
        
        const order = JSON.parse(orderStr);
        
        let hasMatch = false;
        let rawInvoiceTotal = 0;
        
        for (const item of order.purchasedItems) {
          if (item.productId === catalog.productId && item.title === 'Missing Product Name') {
            hasMatch = true;
            item.title = catalog.title;
            const unitPrice = Number(catalog.price);
            item.totalCost = parseFloat((unitPrice * item.qty).toFixed(2));
            rawInvoiceTotal += (unitPrice * item.qty);
          } else {
            const productDetailStr = await redis.hget('lmdb:catalog', item.productId);
            const productDetail = productDetailStr ? JSON.parse(productDetailStr as string) : undefined;
            const unitPrice = productDetail ? Number(productDetail.price) : (item.totalCost / item.qty);
            rawInvoiceTotal += (unitPrice * item.qty);
          }
        }
        
        if (!hasMatch) continue;

        let invoiceTotal = rawInvoiceTotal;
        if (order.customerTier === 'PREMIUM') {
          invoiceTotal = invoiceTotal * 0.9;
        }
        order.invoiceTotal = parseFloat(invoiceTotal.toFixed(2));
        
        const payloadStr = JSON.stringify(order);
        await redis.hset('orders_view', order.orderId, payloadStr);
        await redis.call('JSON.SET', `order:${order.orderId}`, '$', payloadStr);
        await redis.publish('orders_pubsub', payloadStr);
        console.log(`📊 [Self-Heal] Patched stale catalog item for order ${order.orderId}`);
      }
    }
  }
}

// Helper to keep the terminal beautifully rendering our live dashboard
let lastDashboardUpdate = 0;
function updateDashboard(latestEventLog: string) {
  const now = Date.now();
  if (now - lastDashboardUpdate < 2000) return;
  lastDashboardUpdate = now;
  console.clear();
  console.log("==========================================================================");
  console.log("⚡ AWS-STYLE ARCHITECTURE MOCKUP: CQRS MATERIALIZED VIEW STREAM ⚡");
  console.log("==========================================================================");
  console.log(`📡 Latest Cluster Activity: ${latestEventLog}`);
  console.log(`👥 Known Customers Stored in LMDB: ${customersCount}`);
  console.log(`📦 Tracked Catalog Items in LMDB: ${catalogCount}`);
  console.log("==========================================================================");
  console.log("📖 JOINED VIEW RESULTS (The Queryable Read Database Model):");
  
  // Show the last 3 fully aggregated items
  const displaySlice = finalizedOrdersView.slice(-3).reverse();
  if (displaySlice.length === 0) console.log("   (Waiting for orders to stream in...)");
  
  displaySlice.forEach(order => {
    console.log(`\n📄 Order ID: ${order.orderId} [Processed at ${order.processedAt}]`);
    console.log(`👤 Customer: ${order.customerName} | Plan: ${order.customerTier}`);
    console.log(`🛒 Basket Details:`);
    order.purchasedItems.forEach(i => console.log(`   - ${i.qty}x ${i.title} ($${i.totalCost})`));
    console.log(`💰 Final Invoice Total (with tier rules applied): $${order.invoiceTotal}`);
    console.log("--------------------------------------------------------------------------");
  });
}

startCqrsEngine().catch(console.error);
