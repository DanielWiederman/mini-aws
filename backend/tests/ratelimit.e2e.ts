import test, { TestContext } from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';

test('Rate Limiting E2E Tests (Redis Fixed Window)', async (t: TestContext) => {

  await t.test('1. Test Login Endpoint Rate Limiting (by IP)', async () => {
    const testEmail = `ratelimit_user_${Date.now()}@example.com`;
    let responseStatus429Seen = false;

    // Send 7 requests simultaneously (or consecutively very fast)
    for (let i = 1; i <= 7; i++) {
      const res = await fetch(`${API_URL}/customers/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: testEmail, password: 'wrong_password' })
      });

      if (i <= 5) {
        // The first 5 should be processed normally (even if it's 401 Unauthorized for wrong credentials)
        assert.ok(res.status === 401 || res.status === 200, `Expected 401/200, but got ${res.status} on attempt ${i}`);
      } else {
        // The 6th and 7th should be rate limited
        if (res.status === 429) {
          responseStatus429Seen = true;
          const body = await res.json();
          assert.strictEqual(body.error, 'Too many login attempts from this IP. Please try again later.');
        }
      }
    }

    assert.ok(responseStatus429Seen, 'Expected to see at least one 429 Too Many Requests error after 5 attempts');
  });

  await t.test('2. Test Orders Endpoint Rate Limiting (by Customer ID)', async () => {
    const testCustomerId = `ratelimit_cust_${Date.now()}`;
    let responseStatus429Seen = false;

    // Send 7 requests simultaneously
    for (let i = 1; i <= 7; i++) {
      const res = await fetch(`${API_URL}/orders`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Idempotency-Key': `ratelimit-order-${testCustomerId}-${i}` 
        },
        body: JSON.stringify({
          orderId: `order_${Date.now()}_${i}`,
          customerId: testCustomerId,
          items: [{ productId: 'some_product', quantity: 1 }]
        })
      });

      if (i <= 5) {
        // The first 5 should be accepted
        assert.ok(res.status === 202, `Expected 202 Accepted, but got ${res.status} on attempt ${i}`);
      } else {
        // The 6th and 7th should be rate limited
        if (res.status === 429) {
          responseStatus429Seen = true;
          const body = await res.json();
          assert.strictEqual(body.error, 'Too many orders placed recently. Please try again later.');
        }
      }
    }

    assert.ok(responseStatus429Seen, 'Expected to see at least one 429 Too Many Requests error after 5 orders');
  });

});
