import Redis from 'ioredis';
import bcrypt from 'bcryptjs';

const redis = new Redis('redis://localhost:6379');

async function sync() {
  const customerId = 'super_admin_1';
  const email = 'admin@miniaws.com';
  const passwordHash = await bcrypt.hash('admin', 10);
  
  await redis.hset('auth_view', email, JSON.stringify({
    customerId,
    passwordHash,
    role: 'SUPER_ADMIN'
  }));
  
  await redis.hset('customers_view', customerId, JSON.stringify({
    customerId,
    firstName: 'Super',
    lastName: 'Admin',
    email,
    tier: 'PREMIUM',
    role: 'SUPER_ADMIN'
  }));
  console.log('Synced SUPER_ADMIN to Redis auth_view');
  process.exit(0);
}
sync();
