import test from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Orders Service Distributed Saga E2E Lifecycle', async (t) => {
  const timestamp = Date.now();
  const testCustomerId = `test_cust_order_${timestamp}`;
  const testProductId = `test_prod_order_${timestamp}`;
  const testOrderId = `test_order_${timestamp}`;

  await t.test('1. Setup Data: Create Customer & Product', async () => {
    await fetch(`${API_URL}/customers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-c-${timestamp}` },
      body: JSON.stringify({ 
        customerId: testCustomerId, 
        firstName: 'Saga', 
        lastName: 'Test', 
        email: `test-${timestamp}@example.com`, 
        password: 'password123',
        tier: 'STANDARD' 
      })
    });
    
    await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-p-${timestamp}` },
      body: JSON.stringify({ productId: testProductId, title: 'Saga Nike Shoe', price: 100, stockCount: 1 })
    });

    // Wait for prep data to persist
    await delay(3000);
  });

  await t.test('2. Create Order (Triggers Saga)', async () => {
    const payload = {
      orderId: testOrderId,
      customerId: testCustomerId,
      items: [{ productId: testProductId, quantity: 1 }]
    };

    const res = await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-o-${timestamp}` },
      body: JSON.stringify(payload)
    });

    assert.strictEqual(res.status, 202);
  });

  await t.test('3. Wait for Saga Completion', async () => {
    // Saga involves Orders -> Kafka -> Catalog/Customers -> Kafka -> Orders -> Kafka -> CQRS -> Redis
    await delay(4000); 
  });

  await t.test('4. Verify Saga Completed Successfully', async () => {
    const res = await fetch(`${API_URL}/orders/${testOrderId}`);
    assert.strictEqual(res.status, 200);
    
    const order = await res.json();
    assert.strictEqual(order.status, 'COMPLETED', 'Order should be completed after saga stock/customer validation');
    assert.strictEqual(order.customerName, 'Saga Test');
    assert.strictEqual(order.purchasedItems[0].title, 'Saga Nike Shoe');
  });

  await t.test('5. Try to over-order the Nike Shoe (Expect Cancellation)', async () => {
    const overOrderId = `test_order_fail_${timestamp}`;
    await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-o2-${timestamp}` },
      body: JSON.stringify({
        orderId: overOrderId,
        customerId: testCustomerId,
        items: [{ productId: testProductId, quantity: 1 }] // stock is 0 now
      })
    });
    
    await delay(4000);

    const res = await fetch(`${API_URL}/orders/${overOrderId}`);
    assert.strictEqual(res.status, 200);
    
    const order = await res.json();
    assert.strictEqual(order.status, 'CANCELLED', 'Order should be cancelled because stock was exhausted');
  });
});
