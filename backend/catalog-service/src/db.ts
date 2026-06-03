import { Kysely, PostgresDialect } from 'kysely';
import pkg from 'pg';
const { Pool } = pkg;

export interface ProductTable {
  id: import('kysely').Generated<number>;
  product_id: string;
  title: string;
  price: number;
  stock_count: number;
  thumbnail: string;
  image: string;
  description: string | null;
  created_at: import('kysely').Generated<Date>;
  updated_at: import('kysely').Generated<Date>;
  deleted_at: string | null;
}

export interface ScheduledPriceUpdateTable {
  id: import('kysely').Generated<number>;
  product_id: string;
  new_price: number;
  trigger_at: string;
}

export interface Database {
  product: ProductTable;
  scheduled_price_update: ScheduledPriceUpdateTable;
}

const pool = new Pool({
  host: process.env.CATALOG_DB_HOST || 'localhost',
  port: parseInt(process.env.CATALOG_DB_PORT || '5434'),
  user: 'postgres',
  password: 'postgres',
  database: 'catalog_db',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

// Startup health check
pool.query('SELECT 1').then(() => {
  console.log(`🛒 [Catalog] Connecting to Postgres at ${process.env.CATALOG_DB_HOST || 'localhost'}:${process.env.CATALOG_DB_PORT || '5434'}`);
}).catch(() => {
  console.error('❌ FATAL: Cannot connect to Postgres. Refusing to start.');
  process.exit(1);
});

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool })
});
