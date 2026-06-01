const Redis = require('ioredis');
const redis = new Redis('redis://localhost:6379');
async function run() {
  const data = await redis.hget('auth_view', 'admin@miniaws.com');
  console.log('auth_view data:', data);
  process.exit(0);
}
run();
