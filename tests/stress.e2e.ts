import test, { TestContext } from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Saga Concurrency & Stress Test (100 Concurrent Orders for 10 Items)', async (t: TestContext) => {
  const timestamp = Date.now();
  const testProductId = `stress_shoe_${timestamp}`;
  const customerBaseId = `stress_cust_${timestamp}`;

  await t.test('1. Setup Data: Create 100 Customers and 1 Product (Stock: 10)', async () => {
    // 1 Product with 10 stock
    await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Idempotency-Key': `stress-cat-${timestamp}` 
      },
      body: JSON.stringify({ productId: testProductId, title: 'Limited Stress Shoe', price: 200, stockCount: 10 })
    });

    // 100 Customers
    const customerPromises = Array.from({ length: 100 }).map((_, i) => {
      return fetch(`${API_URL}/customers`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Idempotency-Key': `stress-cust-${timestamp}-${i}`
        },
        body: JSON.stringify({ 
          customerId: `${customerBaseId}_${i}`, 
          firstName: `StressUser${i}`,
          lastName: 'Testing',
          email: `stress_user_${timestamp}_${i}@example.com`,
          password: 'password123',
          tier: 'STANDARD'
        })
      });
    });

    await Promise.all(customerPromises);

    // Wait for eventual consistency
    await delay(3000);
  });

  await t.test('2. Blast 100 Concurrent Orders', async () => {
    const orderPromises = Array.from({ length: 100 }).map((_, i) => {
      const orderId = `stress_order_${timestamp}_${i}`;
      return fetch(`${API_URL}/orders`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Idempotency-Key': `stress-order-${timestamp}-${i}`
        },
        body: JSON.stringify({
          orderId,
          customerId: `${customerBaseId}_${i}`,
          items: [{ productId: testProductId, quantity: 1 }]
        })
      });
    });

    // Fire all at the exact same time
    const results = await Promise.all(orderPromises);
    results.forEach(res => assert.strictEqual(res.status, 202));
  });

  await t.test('3. Wait for Saga Resolution Queue', async () => {
    // 100 orders might take a few seconds to fully process through Kafka and Postgres
    await delay(8000);
  });

  await t.test('4. Verify Exactly 10 Successes and 90 Cancellations', async () => {
    let completed = 0;
    let cancelled = 0;
    let missing = 0;
    
    for (let i = 0; i < 100; i++) {
      const orderId = `stress_order_${timestamp}_${i}`;
      const res = await fetch(`${API_URL}/orders/${orderId}`);
      if (res.status === 200) {
        const order = await res.json();
        if (order.status === 'COMPLETED') completed++;
        else if (order.status === 'CANCELLED') cancelled++;
      } else {
        missing++;
      }
    }

    assert.strictEqual(missing, 0, `All 100 orders should be in Redis, but ${missing} were missing`);
    assert.strictEqual(completed, 10, `Exactly 10 orders should have grabbed the stock. Got: ${completed}`);
    assert.strictEqual(cancelled, 90, `Exactly 90 orders should have been cancelled. Got: ${cancelled}`);
  });
});
