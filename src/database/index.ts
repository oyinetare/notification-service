import { Pool } from "pg";

export const pool = new Pool({
  host: "localhost",
  port: 5432,
  database: "notifications",
  user: "user",
  password: "password",
});

export async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id SERIAL PRIMARY KEY,
      notification_id VARCHAR(255) UNIQUE NOT NULL,
      user_id VARCHAR(255) NOT NULL,
      channel VARCHAR(50) NOT NULL,
      title VARCHAR(255),
      message TEXT NOT NULL,
      metadata JSONB,
      status VARCHAR(50) NOT NULL DEFAULT 'pending',
      attempts INT DEFAULT 0,
      error TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_status ON notifications(status);
    CREATE INDEX IF NOT EXISTS idx_created_at ON notifications(created_at);
  `);
}
