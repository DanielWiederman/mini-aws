import pg from 'pg';
import { Kysely, PostgresDialect } from 'kysely';

const { Pool } = pg;

export interface TierTable {
  name: 'STANDARD' | 'PREMIUM';
  price: number;
}

export interface CustomerTable {
  id: import('kysely').Generated<number>;
  customer_id: string;
  first_name: string;
  last_name: string;
  email: string;
}

export interface CustomerTierIndexTable {
  customer_id: number;
  tier_name: 'STANDARD' | 'PREMIUM';
}

export interface Database {
  tier: TierTable;
  customer: CustomerTable;
  customer_tier_index: CustomerTierIndexTable;
}

export const pool = new Pool({
  connectionString: 'postgres://postgres:postgres@localhost:5433/customers_db',
});

const dialect = new PostgresDialect({ pool });
export const db = new Kysely<Database>({ dialect });

export async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS tier (
        name VARCHAR(50) PRIMARY KEY,
        price NUMERIC(10, 2) NOT NULL
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS customer (
        id SERIAL PRIMARY KEY,
        customer_id VARCHAR(50) UNIQUE NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS customer_tier_index (
        customer_id INT REFERENCES customer(id) ON DELETE CASCADE,
        tier_name VARCHAR(50) REFERENCES tier(name) ON DELETE CASCADE,
        PRIMARY KEY (customer_id, tier_name)
      );
    `);

    // Insert default tiers
    await client.query(`
      INSERT INTO tier (name, price)
      VALUES 
        ('STANDARD', 0.00),
        ('PREMIUM', 15.00)
      ON CONFLICT (name) DO NOTHING;
    `);

    console.log('[Postgres] ✅ Database initialized successfully.');
  } finally {
    client.release();
  }
}
