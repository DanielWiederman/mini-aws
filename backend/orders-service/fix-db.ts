import { db } from './src/db.js';
async function fix() {
  await db.schema.dropTable('outbox').ifExists().execute();
  await db.schema.dropTable('order').ifExists().execute();
  await db.schema.dropTable('order_item').ifExists().execute();
  console.log("Tables dropped.");
  process.exit(0);
}
fix();
