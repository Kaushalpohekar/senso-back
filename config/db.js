// src/config/db.js
const { Pool } = require('pg');
require('dotenv').config();
const logger = require('../utils/logger'); // Winston logger

// Create PostgreSQL connection pool
const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 5432,
  max: 10, // Max concurrent connections
  idleTimeoutMillis: 10000, // VAPT: close idle connections quickly (10s)
  connectionTimeoutMillis: 2000 // VAPT: fail fast on broken DBs
});

// VAPT: log unexpected client errors
pool.on('error', (err, client) => {
  logger.error(`[PG ERROR] Unexpected client error: ${err.message}`);
  process.exit(1); // Crash on fatal DB failure
});

// VAPT: keep at least one connection alive to avoid dropouts
setInterval(async () => {
  try {
    await pool.query('SELECT 1'); // Lightweight keep-alive
    //logger.info('[PG] Keep-alive ping successful');
  } catch (err) {
    //logger.warn(`[PG] Keep-alive failed: ${err.message}`);
  }
}, 4000); // Run before 5s idle cut-off

// VAPT: secure wrapper for all queries
async function safeQuery(text, params = []) {
  try {
    const result = await pool.query(text, params);
    return result;
  } catch (err) {
    logger.error(`[PG QUERY ERROR] ${err.message}`);
    throw new Error('Internal Server Error'); // Never leak raw DB errors
  }
}

module.exports = {
  query: safeQuery,
  pool // Export raw pool only if absolutely necessary
};