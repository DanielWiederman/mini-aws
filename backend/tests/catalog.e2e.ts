import test from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Catalog Service E2E Lifecycle (CQRS)', async (t) => {
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
});
