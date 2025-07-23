const { Pool } = require('pg');
require('dotenv').config();
const logger = require('../utils/logger');

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 5432,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 50000,
  statement_timeout: 1000000, 
  query_timeout: 1000000
});

pool.on('error', (err) => {
  logger.error(`[PG ERROR] Unexpected client error: ${err.message}`);
  process.exit(1);
});

setInterval(async () => {
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    logger.warn(`[PG] Keep-alive failed: ${err.message}`);
  }
}, 10000);

async function safeQuery(text, params = []) {
  try {
    return await pool.query(text, params);
  } catch (err) {
    logger.error(`[PG QUERY ERROR] ${err.message}`);
    throw new Error('Internal Server Error');
  }
}

module.exports = {
  query: safeQuery,
  pool,
  connect: () => pool.connect()
};
