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
}

export interface OrderItemTable {
  id: import('kysely').Generated<number>;
  order_id: string;
  product_id: string;
  quantity: number;
}

export interface Database {
  order: OrderTable;
  order_item: OrderItemTable;
}

const pool = new Pool({
  host: 'localhost',
  port: 5435, // Dedicated orders-db
  user: 'postgres',
  password: 'postgres',
  database: 'orders_db'
});

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool })
});
