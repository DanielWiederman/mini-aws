import test, { TestContext } from 'node:test';
import assert from 'node:assert';

const API_URL = 'http://localhost:3000/api';

// Helper to wait for Kafka processing
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('Customers Service E2E Lifecycle (CQRS & Event Sourcing)', async (t: TestContext) => {
  const testCustomerId = `test_cust_${Date.now()}`;
  const testEmail = `alice_${Date.now()}@example.com`;

  await t.test('1. Create Customer Command (Write Side)', async () => {
    const payload = {
      customerId: testCustomerId,
      firstName: 'Alice',
      lastName: 'Smith',
      email: testEmail,
      password: 'my_super_secret_password'
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
    assert.strictEqual(customer.firstName, 'Alice');
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
    let customer;
    for (let i = 0; i < 15; i++) {
      const res = await fetch(`${API_URL}/customers/${testCustomerId}`);
      if (res.status === 200) {
        customer = await res.json();
        if (customer.tier === 'PREMIUM') break;
      }
      await delay(1000);
    }
    assert.strictEqual(customer?.tier, 'PREMIUM', 'Tier should eventually be PREMIUM');
  });

  await t.test('7. Test Login Success', async () => {
    const res = await fetch(`${API_URL}/customers/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: testEmail, password: 'my_super_secret_password' })
    });
    
    assert.strictEqual(res.status, 200);
    const data = await res.json();
    assert.ok(data.token, 'Should return a JWT token');
    assert.strictEqual(data.customerId, testCustomerId);
  });

  await t.test('8. Test Login Failure (Wrong Password)', async () => {
    const res = await fetch(`${API_URL}/customers/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: testEmail, password: 'wrong_password' })
    });
    
    assert.strictEqual(res.status, 401);
  });

  let sessionCookie: string;

  await t.test('9. Try authenticated route without login (Expect 401)', async () => {
    const res = await fetch(`${API_URL}/customers/me`);
    assert.strictEqual(res.status, 401);
  });

  await t.test('10. Login and use session cookie to access authenticated route (Expect 200)', async () => {
    // Login
    const loginRes = await fetch(`${API_URL}/customers/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: testEmail, password: 'my_super_secret_password' })
    });
    assert.strictEqual(loginRes.status, 200);

    // Extract cookie
    const setCookieHeader = loginRes.headers.get('set-cookie');
    assert.ok(setCookieHeader, 'Should receive Set-Cookie header');
    sessionCookie = setCookieHeader.split(';')[0]; // simple extraction of jwt=...

    // Use cookie
    const meRes = await fetch(`${API_URL}/customers/me`, {
      headers: { 'Cookie': sessionCookie }
    });
    assert.strictEqual(meRes.status, 200);
    const data = await meRes.json();
    assert.strictEqual(data.customerId, testCustomerId);
  });

  await t.test('11. Logout (Expect 200)', async () => {
    const logoutRes = await fetch(`${API_URL}/customers/logout`, {
      method: 'POST'
    });
    assert.strictEqual(logoutRes.status, 200);
    
    const setCookieHeader = logoutRes.headers.get('set-cookie');
    assert.ok(setCookieHeader?.includes('jwt=;'), 'Should clear jwt cookie');
  });

  await t.test('12. Try authenticated route with cleared session (Expect 401)', async () => {
    // If the client simulates passing the cleared cookie or no cookie
    const meRes = await fetch(`${API_URL}/customers/me`, {
      headers: { 'Cookie': 'jwt=' }
    });
    assert.strictEqual(meRes.status, 401);
  });

  await t.test('13. Try public API after logout (Expect 200)', async () => {
    const publicRes = await fetch(`${API_URL}/customers/${testCustomerId}`);
    assert.strictEqual(publicRes.status, 200);
    const data = await publicRes.json();
    assert.strictEqual(data.customerId, testCustomerId);
  });
});
