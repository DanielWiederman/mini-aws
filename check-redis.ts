import { Redis } from 'ioredis';
const redis = new Redis();
async function run() {
  const catalog = await redis.hget('catalog_view', 'test_prod_1780380825294');
  console.log("Catalog test_prod_1780380825294:", catalog);
  process.exit(0);
}
run();
