const assert = require('assert');

async function run() {
  console.log('Logging in as super_admin...');
  const res = await fetch('http://127.0.0.1:3000/api/customers/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: 'admin@miniaws.com', password: 'admin' })
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error);
  console.log('Login success!', data.role);
  
  console.log('Creating a dummy admin...');
  const adminRes = await fetch('http://127.0.0.1:3000/api/admins', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${data.token}`, 'Idempotency-Key': 'idemp-1' },
    body: JSON.stringify({
      customerId: 'admin_test_1',
      firstName: 'Test',
      lastName: 'Admin',
      email: 'test_admin@miniaws.com',
      password: 'password'
    })
  });
  console.log('Create admin status:', adminRes.status);
  
  console.log('Testing price schedule...');
  const priceRes = await fetch('http://127.0.0.1:3000/api/catalog/TEST_PROD_1/price-schedule', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${data.token}`, 'Idempotency-Key': 'idemp-2' },
    body: JSON.stringify({
      newPrice: 99.99,
      triggerAt: new Date(Date.now() + 5000).toISOString()
    })
  });
  console.log('Price schedule status:', priceRes.status);
}

run().catch(console.error);
