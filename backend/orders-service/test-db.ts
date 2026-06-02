import { db } from './src/db.js';
async function test() {
  const orders = await db.selectFrom('order').selectAll().execute();
  console.log(orders);
  process.exit(0);
}
test();
