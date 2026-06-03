import { Pool } from 'pg';
import { Kysely, PostgresDialect } from 'kysely';

interface Database {
  system_logs: {
    id: string; // UUID
    level: string;
    service: string;
    message: string;
    error_message: string | null;
    stack_trace: string | null;
    metadata: any | null; // JSONB
    timestamp: Date;
  };
}

export const pool = new Pool({
  host: process.env.LOGS_DB_HOST || 'localhost',
  port: parseInt(process.env.LOGS_DB_PORT || '5436'),
  user: 'user',
  password: 'password',
  database: 'logs_db',
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

// Startup health check
pool.query('SELECT 1').then(() => {
  console.log(`🛒 [Logs] Connecting to Postgres at ${process.env.LOGS_DB_HOST || 'localhost'}:${process.env.LOGS_DB_PORT || '5436'}`);
}).catch(() => {
  console.error('❌ FATAL: Cannot connect to Postgres. Refusing to start.');
  process.exit(1);
});

const dialect = new PostgresDialect({
  pool
});

export const db = new Kysely<Database>({
  dialect,
});
