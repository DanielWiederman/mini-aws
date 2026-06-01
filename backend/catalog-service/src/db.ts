import { Kysely, PostgresDialect } from 'kysely';
import pkg from 'pg';
const { Pool } = pkg;

export interface ProductTable {
  id: import('kysely').Generated<number>;
  product_id: string;
  title: string;
  price: number;
  stock_count: number;
}

export interface Database {
  product: ProductTable;
}

const pool = new Pool({
  host: 'localhost',
  port: 5434,
  user: 'postgres',
  password: 'postgres',
  database: 'catalog_db'
});

export const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool })
});
