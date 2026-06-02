import test, { TestContext } from 'node:test';
import assert from 'node:assert';
import { Client } from 'pg';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Orders Service Distributed Saga E2E Lifecycle', async (t: TestContext) => {
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

  await t.test('6. Order Cancellation Due to Invalid Customer (Compensating Transaction)', async () => {
    const invalidCustOrderId = `test_order_invalid_cust_${timestamp}`;
    const testProductId3 = `test_prod_comp_${timestamp}`;

    // Create a new product specifically for this test
    await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-p3-${timestamp}` },
      body: JSON.stringify({ productId: testProductId3, title: 'Compensation Test Item', price: 50, stockCount: 5 })
    });
    
    await delay(3000);

    // Create an order with a completely invalid customer ID
    await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-o3-${timestamp}` },
      body: JSON.stringify({
        orderId: invalidCustOrderId,
        customerId: 'fake_customer_id_99999',
        items: [{ productId: testProductId3, quantity: 2 }]
      })
    });

    // Wait for the saga to fail, the order to be cancelled, and the compensating transaction to restore stock
    await delay(5000);

    // 1. Verify the order is cancelled
    const orderRes = await fetch(`${API_URL}/orders/${invalidCustOrderId}`);
    assert.strictEqual(orderRes.status, 200);
    const order = await orderRes.json();
    assert.strictEqual(order.status, 'CANCELLED', 'Order should be cancelled due to invalid customer');

    // 2. Verify the stock was restored back to 5
    const catalogRes = await fetch(`${API_URL}/catalog/${testProductId3}`);
    assert.strictEqual(catalogRes.status, 200);
    const product = await catalogRes.json();
    assert.strictEqual(product.stockCount, 5, 'Stock should be restored to 5 via compensating transaction');
  });

  await t.test('7. Idempotency: Duplicate order event is ignored', async () => {
    const idemTimestamp = Date.now();
    const idemCustId = `idem_cust_${idemTimestamp}`;
    const idemProdId = `idem_prod_${idemTimestamp}`;
    const idemOrderId = `idem_order_${idemTimestamp}`;

    await fetch(`${API_URL}/customers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-c-${idemTimestamp}` },
      body: JSON.stringify({ 
        customerId: idemCustId, 
        firstName: 'Idem', 
        lastName: 'Test', 
        email: `idem-${idemTimestamp}@example.com`, 
        password: 'password123',
        tier: 'STANDARD' 
      })
    });

    await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-p-${idemTimestamp}` },
      body: JSON.stringify({ productId: idemProdId, title: 'Idem Shoe', price: 100, stockCount: 10 })
    });

    await delay(3000);

    const payload = {
      orderId: idemOrderId,
      customerId: idemCustId,
      items: [{ productId: idemProdId, quantity: 1 }]
    };

    const idempotencyKey = `req-o-idem-${idemTimestamp}`;

    // POST the same order twice
    const res1 = await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(payload)
    });
    assert.strictEqual(res1.status, 202);

    const res2 = await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(payload)
    });
    assert.strictEqual(res2.status, 202);

    await delay(4000);

    const orderRes = await fetch(`${API_URL}/orders/${idemOrderId}`);
    assert.strictEqual(orderRes.status, 200);
    const order = await orderRes.json();
    assert.strictEqual(order.status, 'COMPLETED');

    const catalogRes = await fetch(`${API_URL}/catalog/${idemProdId}`);
    assert.strictEqual(catalogRes.status, 200);
    const product = await catalogRes.json();
    assert.strictEqual(product.stockCount, 9, 'Stock should be decremented only once');
  });

  await t.test('8. Race Condition: Late STOCK_RESERVED_END on already-CANCELLED order', async () => {
    const raceTimestamp = Date.now();
    const raceProdId = `race_prod_${raceTimestamp}`;
    const raceOrderId = `race_order_${raceTimestamp}`;

    await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-p-${raceTimestamp}` },
      body: JSON.stringify({ productId: raceProdId, title: 'Race Shoe', price: 100, stockCount: 1 })
    });

    await delay(3000);

    await fetch(`${API_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `req-o-${raceTimestamp}` },
      body: JSON.stringify({
        orderId: raceOrderId,
        customerId: 'fake_customer_id_99999',
        items: [{ productId: raceProdId, quantity: 1 }]
      })
    });

    await delay(6000);

    const orderRes = await fetch(`${API_URL}/orders/${raceOrderId}`);
    assert.strictEqual(orderRes.status, 200);
    const order = await orderRes.json();
    assert.strictEqual(order.status, 'CANCELLED');

    const catalogRes = await fetch(`${API_URL}/catalog/${raceProdId}`);
    assert.strictEqual(catalogRes.status, 200);
    const product = await catalogRes.json();
    assert.strictEqual(product.stockCount, 1);
  });

  await t.test('9. Outbox crash recovery: Unprocessed outbox rows are retried by the polling relay', async () => {
    const client = new Client({
      host: 'localhost',
      port: 5435,
      database: 'orders_db',
      user: 'postgres',
      password: 'postgres'
    });
    await client.connect();

    const crashOrderId = `crash_order_${Date.now()}`;
    const payload = {
      eventType: 'ORDER_PENDING_END',
      orderId: crashOrderId,
      items: [{ productId: 'test_prod_1', quantity: 1 }],
      timestamp: new Date().toISOString()
    };

    const res = await client.query(`
      INSERT INTO outbox (topic, key, payload, created_at)
      VALUES ($1, $2, $3, now() - interval '15 seconds')
      RETURNING id
    `, ['orders-topic', crashOrderId, JSON.stringify(payload)]);

    const insertedId = res.rows[0].id;

    await delay(7000);

    const verifyRes = await client.query('SELECT processed_at FROM outbox WHERE id = $1', [insertedId]);
    assert.ok(verifyRes.rows.length > 0);
    assert.notStrictEqual(verifyRes.rows[0].processed_at, null);

    await client.end();
  });
});
