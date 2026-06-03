import test, { TestContext } from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const durationHours = parseFloat(process.argv[2]) || 12;
const durationMs = durationHours * 60 * 60 * 1000;
const startTime = Date.now();
const endTime = startTime + durationMs;

const customers: string[] = [];
const products: string[] = [];
let reqCount = 0;

async function createUser() {
  const customerId = `stress_cust_${Date.now()}_${Math.floor(Math.random()*10000)}`;
  await fetch(`${API_URL}/customers`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `create-cust-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({
      customerId,
      firstName: `Stress`,
      lastName: `User${Math.floor(Math.random()*1000)}`,
      email: `${customerId}@example.com`,
      password: 'password123',
      tier: 'STANDARD'
    })
  });
  customers.push(customerId);
  reqCount++;
}

async function createProduct(stockCount = Math.floor(Math.random() * 100) + 10) {
  const productId = `stress_prod_${Date.now()}_${Math.floor(Math.random()*10000)}`;
  await fetch(`${API_URL}/catalog`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `create-cat-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({
      productId,
      title: `Stress Item ${Math.floor(Math.random()*1000)}`,
      price: Math.floor(Math.random() * 500) + 10,
      stockCount
    })
  });
  products.push(productId);
  reqCount++;
  return productId;
}

async function placeOrder() {
  if (customers.length === 0 || products.length === 0) return;
  const customerId = customers[Math.floor(Math.random() * customers.length)];
  const productId = products[Math.floor(Math.random() * products.length)];
  const orderId = `stress_order_${Date.now()}_${Math.floor(Math.random()*100000)}`;
  
  await fetch(`${API_URL}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `create-order-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({
      orderId,
      customerId,
      items: [{ productId, quantity: 1 }]
    })
  });
  reqCount++;
}

async function restockItem() {
  if (products.length === 0) return;
  const productId = products[Math.floor(Math.random() * products.length)];
  
  await fetch(`${API_URL}/catalog/${productId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `restock-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({
      title: `Stress Item Restocked`,
      price: Math.floor(Math.random() * 500) + 10,
      stockCount: Math.floor(Math.random() * 100) + 50
    })
  });
  reqCount++;
}

async function schedulePriceUpdate() {
  if (products.length === 0) return;
  const productId = products[Math.floor(Math.random() * products.length)];
  const triggerAt = new Date(Date.now() + 60 * 1000).toISOString(); // 1 minute from now
  
  await fetch(`${API_URL}/catalog/${productId}/price-schedule`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `schedule-${Date.now()}-${Math.random()}` },
    body: JSON.stringify({
      newPrice: Math.floor(Math.random() * 500) + 10,
      triggerAt
    })
  });
  reqCount++;
}

async function hotItemDropScenario() {
  console.log(`🔥 Executing HOT ITEM DROP scenario...`);
  // Create a hot product with low stock (e.g. 50)
  const hotProductId = await createProduct(50);
  await delay(2000); // Wait for eventual consistency
  
  // Blast 1000 concurrent orders
  console.log(`🔥 Blasting 1000 concurrent orders for hot product: ${hotProductId}`);
  const orderPromises = [];
  for (let i = 0; i < 1000; i++) {
    // If we don't have enough customers, create on the fly or reuse. Let's reuse existing plus some generated on the fly.
    const custId = customers.length > 0 ? customers[Math.floor(Math.random() * customers.length)] : `dummy_cust_${i}`;
    const orderId = `hot_order_${Date.now()}_${i}`;
    orderPromises.push(
      fetch(`${API_URL}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `hot-order-${Date.now()}-${i}` },
        body: JSON.stringify({
          orderId,
          customerId: custId,
          items: [{ productId: hotProductId, quantity: 1 }]
        })
      })
    );
  }
  
  await Promise.all(orderPromises);
  reqCount += 1000;
  console.log(`🔥 Hot item drop finished.`);
}

console.log(`🚀 Starting ${durationHours} hour stress test...`);

(async () => {
  // Pre-seed some customers and products
  for(let i = 0; i < 50; i++) await createUser();
  for(let i = 0; i < 20; i++) await createProduct();
  
  let lastHotDropTime = Date.now();
  
  while (Date.now() < endTime) {
    const r = Math.random();
    
    try {
      if (r < 0.1) {
        await createUser();
      } else if (r < 0.2) {
        await createProduct();
      } else if (r < 0.7) {
        await placeOrder();
      } else if (r < 0.85) {
        await restockItem();
      } else {
        await schedulePriceUpdate();
      }
      
      // Every 5 minutes, run a hot item drop
      if (Date.now() - lastHotDropTime > 5 * 60 * 1000) {
        await hotItemDropScenario();
        lastHotDropTime = Date.now();
      }
      
      // Delay to avoid completely destroying the local machine
      await delay(50);
      
      if (reqCount % 100 === 0) {
        console.log(`✅ Progress: ${reqCount} requests sent. Time left: ${((endTime - Date.now()) / 1000 / 60).toFixed(2)} mins`);
      }
    } catch (err) {
      console.error(`❌ Request failed:`, err);
    }
  }
  
  console.log(`🎉 Stress test completed! Total requests sent: ${reqCount}`);
})();
