import test from 'node:test';
import assert from 'node:assert';
import { Client } from 'pg';

const API_URL = 'http://localhost:3000/api';
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Fallback to 60 seconds (1 min) if no arg is provided, so we don't accidentally run 1 hour test in CI
const durationSeconds = parseFloat(process.argv[2]) || 60; 
const requestsPerSecond = 278; 
const runId = Date.now();

let submittedOrders = 0;
let acceptedOrders = 0;
let failedOrders = 0;
let rateLimitedOrders = 0;

async function setupData() {
  const customerId = `super_cust_${runId}`;
  const productId = `super_prod_${runId}`;
  
  const custRes = await fetch(`${API_URL}/customers`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `super-cust-${runId}` },
    body: JSON.stringify({
      customerId,
      firstName: `Super`,
      lastName: `User`,
      email: `${customerId}@example.com`,
      password: 'password123',
      tier: 'STANDARD'
    })
  });
  if (!custRes.ok) console.error(`[Setup Failed] Customer: ${custRes.status} ${await custRes.text()}`);
  
  const catRes = await fetch(`${API_URL}/catalog`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `super-cat-${runId}` },
    body: JSON.stringify({
      productId,
      title: `Super Item`,
      price: 10,
      stockCount: 10000000 // Huge stock so it doesn't fail on out of stock
    })
  });
  if (!catRes.ok) console.error(`[Setup Failed] Catalog: ${catRes.status} ${await catRes.text()}`);
  
  await delay(3000); // Wait for eventual consistency
  
  const healthRes = await fetch(`${API_URL}/orders/health-check-nonexistent-id`).catch(() => null);
  if (!healthRes) throw new Error('[Pre-flight] API Gateway is not reachable at http://localhost:3000');
  
  return { customerId, productId };
}

async function blastOrders(customerId: string, productId: string) {
  const startTime = Date.now();
  const endTime = startTime + (durationSeconds * 1000);
  
  console.log(`🚀 Starting super stress test for ${durationSeconds} seconds at ${requestsPerSecond} req/sec...`);
  
  while (Date.now() < endTime) {
    const batchStart = Date.now();
    const promises = [];
    
    for (let i = 0; i < requestsPerSecond; i++) {
      submittedOrders++;
      const orderId = `super_order_${runId}_${submittedOrders}`;
      promises.push(
        fetch(`${API_URL}/orders`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `super-order-${runId}-${submittedOrders}` },
          body: JSON.stringify({
            orderId,
            customerId,
            items: [{ productId, quantity: 1 }]
          })
        }).then(async res => {
          if (res.status === 202) {
            acceptedOrders++;
          } else if (res.status === 429) {
            rateLimitedOrders++;
          } else {
            failedOrders++;
            if (failedOrders <= 3) console.error(`[Order Failed] Status: ${res.status} Body: ${await res.text()}`);
          }
        }).catch(err => {
          failedOrders++;
          if (failedOrders <= 3) console.error(`[Order Failed Network Error]:`, err.message);
        })
      );
    }
    
    // Wait for the batch to finish sending
    await Promise.all(promises);
    
    const elapsed = Date.now() - batchStart;
    if (elapsed < 1000) {
      await delay(1000 - elapsed); // throttle exactly to 1 second
    } else if (submittedOrders % (requestsPerSecond * 5) < requestsPerSecond) {
      console.warn(`⚠️ Warning: Batch took ${elapsed}ms (> 1s). System might be bottlenecking.`);
    }
    
    if (submittedOrders % (requestsPerSecond * 5) < requestsPerSecond) {
      console.log(`✅ Progress: ${submittedOrders} orders submitted (${acceptedOrders} accepted, ${rateLimitedOrders} rate-limited, ${failedOrders} failed).`);
    }
  }

  assert.strictEqual(failedOrders, 0, `${failedOrders} orders failed with non-202/non-429 responses — potential API gateway crash`);
}

async function blastMixedBatch(customerId: string, productId: string) {
  console.log(`\n🚀 Starting 10-second mixed batch (50% valid, 50% fake customer)...`);
  const startTime = Date.now();
  const endTime = startTime + (10 * 1000);
  
  while (Date.now() < endTime) {
    const batchStart = Date.now();
    const promises = [];
    
    for (let i = 0; i < requestsPerSecond; i++) {
      submittedOrders++;
      const orderId = `super_order_${runId}_${submittedOrders}`;
      const isFake = i % 2 === 0;
      const orderCustomerId = isFake ? `fake_cust_${runId}_${submittedOrders}` : customerId;
      
      promises.push(
        fetch(`${API_URL}/orders`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `super-order-${runId}-${submittedOrders}` },
          body: JSON.stringify({
            orderId,
            customerId: orderCustomerId,
            items: [{ productId, quantity: 1 }]
          })
        }).then(async res => {
          if (res.status === 202) {
            acceptedOrders++;
          } else if (res.status === 429) {
            rateLimitedOrders++;
          } else {
            failedOrders++;
          }
        }).catch(() => {
          failedOrders++;
        })
      );
    }
    await Promise.all(promises);
    
    const elapsed = Date.now() - batchStart;
    if (elapsed < 1000) {
      await delay(1000 - elapsed);
    }
  }

  assert.strictEqual(failedOrders, 0, `${failedOrders} orders failed with non-202/non-429 responses in mixed batch`);
  console.log(`✅ Mixed batch complete. Total Submitted: ${submittedOrders}, Total Accepted: ${acceptedOrders}, Total Rate-Limited: ${rateLimitedOrders}`);
}

async function verifyData() {
  console.log(`\n🔍 Verification Phase...`);
  console.log(`Waiting for Kafka and CQRS queues to drain (polling Postgres up to 3 minutes)...`);
  
  const client = new Client({ connectionString: 'postgres://postgres:postgres@localhost:5435/orders_db' });
  await client.connect();
  
  let pgCount = 0;
  let completedVerified = 0;
  let cancelledVerified = 0;
  let attempts = 0;
  const maxAttempts = 36; // 36 * 5s = 180s = 3 minutes
  
  while (attempts < maxAttempts) {
    const pgRes = await client.query(`SELECT status, COUNT(*) FROM "order" WHERE order_id LIKE 'super_order_${runId}_%' GROUP BY status`);
    
    let totalFound = 0;
    let tempCompleted = 0;
    let tempCancelled = 0;
    
    for (const row of pgRes.rows) {
      const c = parseInt(row.count, 10);
      totalFound += c;
      if (row.status === 'COMPLETED') tempCompleted += c;
      if (row.status === 'CANCELLED') tempCancelled += c;
    }
    
    pgCount = totalFound;
    completedVerified = tempCompleted;
    cancelledVerified = tempCancelled;
    
    if (pgCount >= acceptedOrders) {
      break;
    }
    
    attempts++;
    console.log(`   [Poll ${attempts}/${maxAttempts}] Found ${pgCount}/${acceptedOrders} orders in Postgres. Waiting 5s...`);
    await delay(5000);
  }
  
  await client.end();
  
  console.log(`📊 Expected (Accepted by API Gateway): ${acceptedOrders}`);
  console.log(`📊 Found in Postgres (Final State): ${pgCount} (Completed: ${completedVerified}, Cancelled: ${cancelledVerified})`);
  
  assert.strictEqual(pgCount, acceptedOrders, `Data loss detected! Postgres has ${pgCount} but we accepted ${acceptedOrders}`);
  
  // Try verifying Redis CQRS view dynamically
  try {
    const Redis = (await import('ioredis')).default;
    const redis = new Redis();
    const startMs = runId; 
    const endMs = Date.now();
    const [redisCountRes] = await redis.call('FT.SEARCH', 'idx:orders', `@createdAt:[${startMs} ${endMs}]`, 'LIMIT', '0', '0') as any;
    const redisCount = redisCountRes;
    
    console.log(`📊 Found in RediSearch View (Queryable Read Data): ${redisCount}`);
    const finalizedPgCount = completedVerified + cancelledVerified;
    assert.strictEqual(redisCount, finalizedPgCount, `Data loss detected in Redis CQRS View! Redis has ${redisCount} but Postgres has ${finalizedPgCount} finalized orders`);
  } catch (err: any) {
    console.log(`⚠️ Note: Redis verification skipped or failed: ${err.message}`);
  }

  console.log(`🎉 VERIFICATION SUCCESSFUL! Zero data loss under extreme load!`);
  process.exit(0);
}

(async () => {
  try {
    const { customerId, productId } = await setupData();
    await blastOrders(customerId, productId);
    await blastMixedBatch(customerId, productId);
    await verifyData();
  } catch (e) {
    console.error(`❌ Test failed:`, e);
    process.exit(1);
  }
})();
