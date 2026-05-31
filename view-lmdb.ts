// @ts-nocheck
import { open } from 'lmdb';

const dbPath = process.argv[2];

if (!dbPath) {
  console.error('❌ Usage: pnpm dlx tsx view-lmdb.ts <path-to-db>');
  console.error('💡 Example: pnpm dlx tsx view-lmdb.ts ./derived-view-service/db/customers');
  process.exit(1);
}

try {
  // Open the database in read-only mode to prevent locking issues with running services
  const db = open({ path: dbPath, readOnly: true });
  
  console.log(`\n📂 Opened LMDB Database: ${dbPath}`);
  console.log('==================================================');

  let count = 0;
  // Iterate through all key-value pairs in the database
  for (const { key, value } of db.getRange()) {
    console.log(`\n🔑 Key: ${key}`);
    console.log(JSON.stringify(value, null, 2));
    count++;
  }
  
  console.log('\n==================================================');
  console.log(`✅ Total Records Found: ${count}\n`);

} catch (err) {
  console.error(`❌ Failed to open LMDB at ${dbPath}. Is the path correct?`, err);
}
