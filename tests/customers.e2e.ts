import test from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';

// Helper to wait for Kafka processing
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Customers Service E2E Lifecycle (CQRS & Event Sourcing)', async (t) => {
  const testCustomerId = `test_cust_${Date.now()}`;

  await t.test('1. Create Customer Command (Write Side)', async () => {
    const payload = {
      customerId: testCustomerId,
      firstName: 'Integration',
      lastName: 'Test',
      email: `test-${testCustomerId}@example.com`
    };

    const res = await fetch(`${API_URL}/customers`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Idempotency-Key': `req-${testCustomerId}-create`
      },
      body: JSON.stringify(payload)
    });

    assert.strictEqual(res.status, 202, 'Should return 202 Accepted');
  });

  await t.test('2. Wait for Eventual Consistency', async () => {
    // Wait for Kafka -> Worker -> DB -> Kafka -> CQRS -> Redis
    await delay(3000); 
  });

  await t.test('3. Query Customer (Read Side)', async () => {
    const res = await fetch(`${API_URL}/customers/${testCustomerId}`);
    assert.strictEqual(res.status, 200, 'Should find the customer in Redis materialized view');
    
    const customer = await res.json();
    assert.strictEqual(customer.firstName, 'Integration');
    assert.strictEqual(customer.tier, 'STANDARD', 'Default tier should be STANDARD');
  });

  await t.test('4. Upgrade Tier Command', async () => {
    const res = await fetch(`${API_URL}/customers/${testCustomerId}/tier`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Idempotency-Key': `req-${testCustomerId}-upgrade`
      },
      body: JSON.stringify({ tier: 'PREMIUM' })
    });

    assert.strictEqual(res.status, 202, 'Should return 202 Accepted');
  });

  await t.test('5. Wait for Eventual Consistency', async () => {
    await delay(3000); 
  });

  await t.test('6. Verify Tier Upgrade (Read Side)', async () => {
    const res = await fetch(`${API_URL}/customers/${testCustomerId}`);
    assert.strictEqual(res.status, 200);
    
    const customer = await res.json();
    assert.strictEqual(customer.tier, 'PREMIUM', 'Tier should be updated to PREMIUM');
  });
});
