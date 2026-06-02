import { db } from './src/db.js';
async function run() {
  const product = await db.selectFrom('product').selectAll().where('product_id', '=', 'catalog-e2e-12345').executeTakeFirst();
  console.log(product);
  process.exit(0);
}
run();
