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

const dialect = new PostgresDialect({
  pool: new Pool({
    database: 'logs_db',
    host: 'localhost',
    user: 'user',
    password: 'password',
    port: 5436, // Note the port for logs-db
    max: 10,
  })
});

export const db = new Kysely<Database>({
  dialect,
});
