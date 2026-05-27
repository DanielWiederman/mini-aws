// derived-view-service/src/cqrs-engine.ts
import { Kafka, Consumer } from 'kafkajs';
import { CustomerEvent, CatalogEvent, OrderEvent } from './interfaces';

const kafka = new Kafka({
  clientId: 'derived-view-service',
  brokers: ['localhost:9092']
});

// A single consumer group that subscribes to multiple topics
const consumer: Consumer = kafka.consumer({ groupId: 'cqrs-view-group' });

// --- LOCAL STATE STORES (Simulating local KTables) ---
const customerTable: { [id: string]: CustomerEvent } = {};
const catalogTable: { [id: string]: CatalogEvent } = {};

// --- THE DERIVED READ MODEL (CQRS Materialized View) ---
interface ReadModelOrderSummary {
  orderId: string;
  customerName: string;
  customerTier: string;
  purchasedItems: { title: string; qty: number; totalCost: number }[];
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
        customerTable[customer.customerId] = customer;
        updateDashboard(`Customer updated: ${customer.fullName} (${customer.tier})`);
      }

      // ROUTE 2: Maintain the Catalog local state table (Live price tracking)
      else if (topic === 'catalog-topic') {
        const product = rawData as CatalogEvent;
        catalogTable[product.productId] = product;
        updateDashboard(`Catalog updated: ${product.title} price is now $${product.price}`);
      }

      // ROUTE 3: The Order arrives! Execute real-time stream processing join logic
      else if (topic === 'orders-topic') {
        const rawOrder = rawData as OrderEvent;
        
        // 1. Fetch details from local tables (No HTTP API requests needed!)
        const customerProfile = customerTable[rawOrder.customerId];
        const customerName = customerProfile ? customerProfile.fullName : 'Unknown Customer';
        const customerTier = customerProfile ? customerProfile.tier : 'STANDARD';

        let invoiceTotal = 0;
        const purchasedItems = rawOrder.items.map(item => {
          const productDetail = catalogTable[item.productId];
          const productTitle = productDetail ? productDetail.title : 'Missing Product Name';
          const currentPrice = productDetail ? productDetail.price : 0;
          const itemCost = currentPrice * item.quantity;
          
          invoiceTotal += itemCost;

          return {
            title: productTitle,
            qty: item.quantity,
            totalCost: parseFloat(itemCost.toFixed(2))
          };
        });

        // 2. Apply business rules dynamically based on our joined state (e.g., Premium 10% discount)
        if (customerTier === 'PREMIUM') {
          invoiceTotal = invoiceTotal * 0.9;
        }

        // 3. Save directly to the materialized view
        finalizedOrdersView.push({
          orderId: rawOrder.orderId,
          customerName,
          customerTier,
          purchasedItems,
          invoiceTotal: parseFloat(invoiceTotal.toFixed(2)),
          processedAt: new Date().toLocaleTimeString()
        });

        updateDashboard(`New Order Processed: ${rawOrder.orderId}`);
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
  console.log(`👥 Known Customers Stored Locally: ${Object.keys(customerTable).length}`);
  console.log(`📦 Tracked Catalog Items In-Memory: ${Object.keys(catalogTable).length}`);
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
