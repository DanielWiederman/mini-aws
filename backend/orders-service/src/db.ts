import { Kysely, PostgresDialect } from 'kysely';
import pkg from 'pg';
const { Pool } = pkg;

export interface OrderTable {
  id: import('kysely').Generated<number>;
  order_id: string;
  customer_id: string;
  status: 'PENDING' | 'COMPLETED' | 'CANCELLED';
  stock_status: 'PENDING' | 'RESERVED' | 'DENIED';
  customer_status: 'PENDING' | 'VALID' | 'INVALID';
  created_at: import('kysely').Generated<Date>;
  updated_at: import('kysely').Generated<Date>;
}

export interface OrderItemTable {
  id: import('kysely').Generated<number>;
  order_id: string;
  product_id: string;
  quantity: number;
}

export interface OutboxTable {
  id: import('kysely').Generated<number>;
  topic: string;
  key: string | null;
  payload: any;
  event_id: string | null;
  created_at: import('kysely').Generated<Date>;
  processed_at: Date | null;
}

export interface Database {
  order: OrderTable;
  order_item: OrderItemTable;
  outbox: OutboxTable;
}

const pool = new Pool({
  host: 'localhost',
  port: 5435, // Dedicated orders-db
  user: 'postgres',
  password: 'postgres',
  database: 'orders_db',
  max: 50,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool })
});
