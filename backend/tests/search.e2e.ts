import test from 'node:test';
import assert from 'node:assert';

const API_GATEWAY = 'http://localhost:3000';
let adminToken = '';

test('Store Management Search E2E', async (t) => {
  
  await t.test('1. Setup Admin Token', async () => {
    const loginRes = await fetch(`${API_GATEWAY}/api/customers/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Test-IP': `10.0.99.99` },
      body: JSON.stringify({ email: 'admin@miniaws.com', password: 'admin' })
    });
    
    if (loginRes.ok) {
      const data = await loginRes.json();
      adminToken = data.token;
    }
  });

  await t.test('2. Test Orders Text Search (Query "q")', async () => {
    const res = await fetch(`${API_GATEWAY}/api/orders?q=Saga&limit=5`, {
      headers: adminToken ? { 'Authorization': `Bearer ${adminToken}` } : {}
    });
    
    assert.strictEqual(res.status, 200, `Expected 200, got ${res.status}`);
    const data = await res.json();
    assert(Array.isArray(data.data), 'Expected data to be an array');
    
    if (data.data.length > 0) {
      const first = data.data[0];
      const match = first.orderId.includes('saga') || 
                    first.customerName.toLowerCase().includes('saga') || 
                    first.purchasedItems.some((i: any) => i.title.toLowerCase().includes('saga'));
      assert(match, 'Search result should match query "Saga"');
    }
  });

  await t.test('3. Test Orders Date Range Search', async () => {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date();
    end.setHours(23, 59, 59, 999);
    
    const res = await fetch(`${API_GATEWAY}/api/orders?startDate=${start.toISOString()}&endDate=${end.toISOString()}`, {
      headers: adminToken ? { 'Authorization': `Bearer ${adminToken}` } : {}
    });
    
    assert.strictEqual(res.status, 200);
    const data = await res.json();
    assert(Array.isArray(data.data), 'Expected data to be an array');
    
    if (data.data.length > 0) {
      const first = data.data[0];
      assert(first.createdAt, 'Order must have a createdAt property');
      assert.strictEqual(typeof first.createdAt, 'number', 'createdAt must be a numeric Unix ms timestamp');
      assert(first.createdAt >= start.getTime() && first.createdAt <= end.getTime(), 'Order date should be within range');
    }
  });

  await t.test('4. Test Orders Status Filter', async () => {
    const res = await fetch(`${API_GATEWAY}/api/orders?status=COMPLETED&limit=5`, {
      headers: adminToken ? { 'Authorization': `Bearer ${adminToken}` } : {}
    });
    
    assert.strictEqual(res.status, 200);
    const data = await res.json();
    
    if (data.data.length > 0) {
      data.data.forEach((order: any) => {
        assert.strictEqual(order.status, 'COMPLETED', 'All returned orders must be COMPLETED');
      });
    }
  });

  await t.test('5. Test Orders Combined Filters (Text + Status)', async () => {
    const res = await fetch(`${API_GATEWAY}/api/orders?q=Saga&status=COMPLETED&limit=5`, {
      headers: adminToken ? { 'Authorization': `Bearer ${adminToken}` } : {}
    });
    
    assert.strictEqual(res.status, 200);
    const data = await res.json();
    
    if (data.data.length > 0) {
      data.data.forEach((order: any) => {
        assert.strictEqual(order.status, 'COMPLETED', 'Order must be COMPLETED');
        const match = order.orderId.includes('saga') || 
                      order.customerName.toLowerCase().includes('saga') || 
                      order.purchasedItems.some((i: any) => i.title.toLowerCase().includes('saga'));
        assert(match, 'Search result should match query "Saga"');
      });
    }
  });
});
