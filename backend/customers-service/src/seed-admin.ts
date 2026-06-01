import { db, initDb } from './db.js';
import bcrypt from 'bcryptjs';
import { sendTraced } from 'shared-contracts';
import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'seed-admin', brokers: ['localhost:9092'] });
const producer = kafka.producer();

async function seedAdmin() {
  await initDb();
  
  const customerId = 'super_admin_1';
  const email = 'admin@miniaws.com';
  const password = 'admin';
  const firstName = 'Super';
  const lastName = 'Admin';
  
  const passwordHash = await bcrypt.hash(password, 10);
  
  try {
    await db.transaction().execute(async (trx) => {
      const existing = await trx.selectFrom('customer').where('email', '=', email).select('id').executeTakeFirst();
      
      if (existing) {
        console.log(`[Seed] Admin ${email} already exists. Updating password and role.`);
        await trx.updateTable('customer')
          .set({ password_hash: passwordHash, role: 'SUPER_ADMIN' })
          .where('id', '=', existing.id)
          .execute();
      } else {
        const newAdmin = await trx.insertInto('customer')
          .values({
            customer_id: customerId,
            first_name: firstName,
            last_name: lastName,
            email: email,
            password_hash: passwordHash,
            role: 'SUPER_ADMIN'
          })
          .returning('id')
          .executeTakeFirstOrThrow();
          
        await trx.insertInto('customer_tier_index')
          .values({
            customer_id: newAdmin.id,
            tier_name: 'PREMIUM'
          })
          .execute();
          
        console.log(`[Seed] Created SUPER_ADMIN ${email}`);
      }
    });

    // Manually emit the event to sync CQRS View!
    await producer.connect();
    const event = {
      eventType: 'CUSTOMER_UPDATE_END',
      customerId,
      firstName,
      lastName,
      email,
      tier: 'PREMIUM',
      passwordHash,
      role: 'SUPER_ADMIN'
    };
    await sendTraced(producer, 'customer-topic', [
      { key: customerId, value: JSON.stringify(event) }
    ]);

    console.log('[Seed] Admin user seeded and event emitted successfully.');

  } catch (err) {
    console.error('[Seed] Failed to seed admin', err);
  } finally {
    process.exit(0);
  }
}

seedAdmin();
