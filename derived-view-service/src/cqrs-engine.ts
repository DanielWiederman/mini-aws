import { Kafka, Consumer } from 'kafkajs';
import { CustomerEvent, CatalogEvent, OrderEvent } from 'shared-contracts';
import { open } from 'lmdb';
import Redis from 'ioredis';

const redis = new Redis('redis://localhost:6379');

const kafka = new Kafka({
  clientId: 'derived-view-service',
  brokers: ['localhost:9092']
});

// A single consumer group that subscribes to multiple topics
const consumer: Consumer = kafka.consumer({ groupId: 'cqrs-view-group' });

// --- LOCAL STATE STORES (Backed by LMDB for persistence and high performance) ---
const customerTable = open({ path: './db/customers', compression: true });
const catalogTable = open({ path: './db/catalog', compression: true });

let customersCount = [...customerTable.getKeys()].length;
let catalogCount = [...catalogTable.getKeys()].length;

// --- THE DERIVED READ MODEL (CQRS Materialized View) ---
interface ReadModelOrderSummary {
  orderId: string;
  status?: string;
  customerName: string;
  customerTier: string;
  purchasedItems: { productId: string; title: string; qty: number; totalCost: number }[];
  invoiceTotal: number;
  processedAt: string;
}
const finalizedOrdersView: ReadModelOrderSummary[] = [];

async function startCqrsEngine() {
  await consumer.connect();
  
  // Subscribe to all 3 upstream data feeds
  await consumer.subscribe({ topics: ['customer-topic', 'catalog-topic', 'orders-topic'], fromBeginning: true });
  console.log('📊 CQRS Derived View Engine Online. Waiting to process streams...');

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      const rawData = JSON.parse(message.value.toString());

      // ROUTE 1: Maintain the Customer local state table
      if (topic === 'customer-topic') {
        const customer = rawData as CustomerEvent;
        // Only process completed domain events
        if (!customer.eventType?.endsWith('_END')) return;

        const isNew = customerTable.get(customer.customerId) === undefined;
        await customerTable.put(customer.customerId, customer);
        
        // Expose public profile (without password hash)
        const publicProfile = {
          customerId: customer.customerId,
          firstName: customer.firstName,
          lastName: customer.lastName,
          email: customer.email,
          tier: customer.tier
        };
        await redis.hset('customers_view', customer.customerId, JSON.stringify(publicProfile));

        // Store auth details securely
        if (customer.passwordHash) {
          await redis.hset('auth_view', customer.email, JSON.stringify({
            customerId: customer.customerId,
            passwordHash: customer.passwordHash
          }));
        }
        
        updateDashboard(isNew ? `New Customer: ${customer.firstName}` : `Tier Upgraded: ${customer.firstName} -> ${customer.tier}`);
      }

      // ROUTE 2: Maintain the Catalog local state table (Live price tracking)
      else if (topic === 'catalog-topic') {
        const catalog = rawData as CatalogEvent;
        if (!catalog.eventType?.endsWith('_END')) return;

        await catalogTable.put(catalog.productId, catalog);
        
        // Phase 2: Sync Materialized View to Redis
        await redis.hset('catalog_view', catalog.productId, JSON.stringify(catalog));

        updateDashboard(`Catalog updated: ${catalog.title} ($${catalog.price})`);
      }

      // ROUTE 3: The Order arrives! Execute real-time stream processing join logic
      else if (topic === 'orders-topic') {
        const rawOrder = rawData as OrderEvent;
        if (!rawOrder.eventType?.endsWith('_END')) return;
        if (['STOCK_RESERVED_END', 'STOCK_DENIED_END', 'CUSTOMER_VALIDATED_END', 'CUSTOMER_INVALID_END'].includes(rawOrder.eventType)) return;
        
        // 1. Fetch details from LMDB synchronously (Blazing fast reads)
        const customerProfile = customerTable.get(rawOrder.customerId!) as CustomerEvent | undefined;
        const customerName = customerProfile ? `${customerProfile.firstName} ${customerProfile.lastName}` : 'Unknown Customer';
        const customerTier = customerProfile ? customerProfile.tier : 'STANDARD';

        let invoiceTotal = 0;
        const purchasedItems = (rawOrder.items || []).map(item => {
          const productDetail = catalogTable.get(item.productId) as CatalogEvent | undefined;
          const productTitle = productDetail ? productDetail.title : 'Missing Product Name';
          const currentPrice = productDetail ? productDetail.price : 0;
          const itemCost = currentPrice * item.quantity;
          
          invoiceTotal += itemCost;

          return {
            productId: item.productId,
            title: productTitle,
            qty: item.quantity,
            totalCost: parseFloat(itemCost.toFixed(2))
          };
        });

        // 2. Apply business rules dynamically based on our joined state
        if (customerTier === 'PREMIUM') {
          invoiceTotal = invoiceTotal * 0.9;
        }

        const materializedOrder = {
          orderId: rawOrder.orderId,
          status: rawOrder.status,
          customerName,
          customerTier,
          purchasedItems,
          invoiceTotal: parseFloat(invoiceTotal.toFixed(2)),
          processedAt: new Date().toLocaleTimeString()
        };

        // 3. Save directly to the materialized view in Redis
        await redis.hset('orders_view', rawOrder.orderId, JSON.stringify(materializedOrder));

        finalizedOrdersView.push(materializedOrder);
        updateDashboard(`Order ${rawOrder.orderId} is now ${rawOrder.status}`);
      }
    }
  });
}

// Helper to keep the terminal beautifully rendering our live dashboard
function updateDashboard(latestEventLog: string) {
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
