import { Redis } from 'ioredis';
const redis = new Redis();
async function run() {
  const ordersMap = await redis.hgetall('orders_view');
  const orders = Object.values(ordersMap).map(o => JSON.parse(o));
  orders.sort((a, b) => {
    const tsA = parseInt(a.orderId.split('_').pop() || '0', 10);
    const tsB = parseInt(b.orderId.split('_').pop() || '0', 10);
    return tsB - tsA;
  });
  console.log("Total orders:", orders.length);
  console.log("First 5 orders:", orders.slice(0, 5).map(o => ({ id: o.orderId, status: o.status })));
  process.exit(0);
}
run();
