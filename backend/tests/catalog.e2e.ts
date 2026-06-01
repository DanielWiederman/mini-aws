import test, { TestContext } from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://127.0.0.1:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Catalog Service E2E Lifecycle (CQRS)', async (t: TestContext) => {
  const testProductId = `test_prod_${Date.now()}`;

  await t.test('1. Create Product Command', async () => {
    const payload = {
      productId: testProductId,
      title: 'E2E Test Product',
      price: 19.99,
      stockCount: 100
    };

    const res = await fetch(`${API_URL}/catalog`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Idempotency-Key': `req-${testProductId}-create`
      },
      body: JSON.stringify(payload)
    });

    assert.strictEqual(res.status, 202);
  });

  await t.test('2. Wait for Eventual Consistency', async () => {
    await delay(3000); 
  });

  await t.test('3. Query Product (Read Side)', async () => {
    const res = await fetch(`${API_URL}/catalog/${testProductId}`);
    assert.strictEqual(res.status, 200);
    
    const product = await res.json();
    assert.strictEqual(product.title, 'E2E Test Product');
    assert.strictEqual(product.price, 19.99);
  });

  await t.test('4. Update Price Command', async () => {
    const res = await fetch(`${API_URL}/catalog/${testProductId}/price`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Idempotency-Key': `req-${testProductId}-price`
      },
      body: JSON.stringify({ price: 15.00 })
    });

    assert.strictEqual(res.status, 202);
  });

  await t.test('5. Wait for Eventual Consistency', async () => {
    await delay(3000); 
  });

  await t.test('6. Verify Price Update (Read Side)', async () => {
    const res = await fetch(`${API_URL}/catalog/${testProductId}`);
    assert.strictEqual(res.status, 200);
    
    const product = await res.json();
    assert.strictEqual(product.price, 15.00);
  });

  let adminToken = '';
  await t.test('7. Login as Admin', async () => {
    const res = await fetch(`${API_URL}/customers/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'admin@miniaws.com', password: 'admin' })
    });
    
    // In our local dev environment, admin@miniaws.com is seeded
    if (res.status === 200) {
      const data = await res.json();
      adminToken = data.token;
    } else {
      console.warn('Skipping scheduled price tests: No admin user seeded');
    }
  });

  await t.test('8. Schedule Price Update (BullMQ Queue Test)', async () => {
    if (!adminToken) return;

    const triggerAt = new Date(Date.now() + 4000).toISOString(); // 4 seconds in the future
    
    const res = await fetch(`${API_URL}/catalog/${testProductId}/price-schedule`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${adminToken}`,
        'Idempotency-Key': `req-${testProductId}-price-schedule`
      },
      body: JSON.stringify({ newPrice: 9.99, triggerAt })
    });

    assert.strictEqual(res.status, 202);
  });

  await t.test('9. Verify Price has NOT changed yet (Queue is holding job)', async () => {
    if (!adminToken) return;
    
    await delay(1000); // Wait 1s
    const res = await fetch(`${API_URL}/catalog/${testProductId}`);
    const product = await res.json();
    assert.strictEqual(product.price, 15.00, 'Price should still be 15.00 since schedule has not triggered');
  });

  await t.test('10. Wait for BullMQ Scheduled Job Execution', async () => {
    if (!adminToken) return;
    
    // Wait another 4 seconds so the 4-second delay passes, plus CQRS sync time
    await delay(4000); 
  });

  await t.test('11. Verify Price HAS changed (Queue executed job)', async () => {
    if (!adminToken) return;

    const res = await fetch(`${API_URL}/catalog/${testProductId}`);
    const product = await res.json();
    assert.strictEqual(product.price, 9.99, 'Price should now be updated to 9.99');
  });
});
