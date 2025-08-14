const { Pool } = require('pg');
const cron = require('node-cron');
require('dotenv').config({ path: __dirname + '/../.env' });

const pgPool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: 5432,
  ssl: { rejectUnauthorized: false },
  max: 100,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pgPool.on('connect', () => console.info('✅ PostgreSQL connected'));
pgPool.on('error', (err) => console.error('❌ PostgreSQL error:', err.message));

const BATCH_SIZE = 50000;

async function getDistinctDevices() {
  const res = await pgPool.query(
    `SELECT DISTINCT deviceuid 
     FROM senso.senso_data
     WHERE "timestamp" >= NOW() - INTERVAL '30 minutes'`
  );
  return res.rows.map((r) => r.deviceuid);
}

async function insertBatch(deviceBatch) {
  if (!deviceBatch || deviceBatch.length === 0) return { rowCount: 0 };
  const query = `
    WITH agg AS (
      SELECT
        deviceuid,
        DATE_TRUNC('minute', "timestamp")
          - ((EXTRACT(MINUTE FROM "timestamp")::int % 30) * INTERVAL '1 minute') AS bucket_start_time,
        ROUND(AVG(temperature)::numeric, 1)   AS temperature,
        ROUND(AVG(humidity)::numeric, 1)      AS humidity,
        ROUND(AVG(flowrate)::numeric, 1)      AS flowrate,
        ROUND(AVG(temperaturer)::numeric, 1)  AS temperaturer,
        ROUND(AVG(temperaturey)::numeric, 1)  AS temperaturey,
        ROUND(AVG(temperatureb)::numeric, 1)  AS temperatureb,
        ROUND(AVG(pressure)::numeric, 1)      AS pressure,
        MAX(totalvolume)                      AS totalvolume
      FROM senso.senso_data
      WHERE "timestamp" >= NOW() - INTERVAL '30 minutes'
        AND "timestamp" <  NOW()
        AND deviceuid = ANY($1)
      GROUP BY deviceuid, bucket_start_time
    )
    INSERT INTO senso.senso_data_30_min (
      deviceuid, "timestamp", temperature, humidity, flowrate,
      temperaturer, temperaturey, temperatureb, pressure, totalvolume
    )
    SELECT
      a.deviceuid,
      a.bucket_start_time,
      a.temperature,
      a.humidity,
      a.flowrate,
      a.temperaturer,
      a.temperaturey,
      a.temperatureb,
      a.pressure,
      a.totalvolume
    FROM agg a
    WHERE NOT EXISTS (
      SELECT 1
      FROM senso.senso_data_30_min t
      WHERE t.deviceuid = a.deviceuid
        AND t."timestamp" = a.bucket_start_time
    );
  `;
  const res = await pgPool.query(query, [deviceBatch]);
  return res;
}

async function insertCleanData() {
  const startedAt = new Date().toISOString();
  try {
    const devices = await getDistinctDevices();
    if (!devices.length) {
      console.warn(`[${startedAt}] ⚠ No active devices in last 30 minutes`);
      return;
    }
    console.info(`[${startedAt}] Found ${devices.length} active devices`);
    let totalInserted = 0;
    for (let i = 0; i < devices.length; i += BATCH_SIZE) {
      const batch = devices.slice(i, i + BATCH_SIZE);
      const res = await insertBatch(batch);
      totalInserted += res.rowCount || 0;
      console.info(
        `[${new Date().toISOString()}] ✅ Batch inserted missing buckets: ${batch.length} devices, ${res.rowCount || 0} rows`
      );
    }
    console.info(
      `[${new Date().toISOString()}] ✅ Aggregation complete. Total rows inserted: ${totalInserted}`
    );
  } catch (err) {
    console.error(`[${startedAt}] ❌ Aggregation Error:`, err.stack || err.message);
  }
}

cron.schedule('*/30 * * * *', insertCleanData);
console.info('✅ Cron initialized: running every 30 minutes');

insertCleanData().catch(() => {});

const shutdown = async (signal) => {
  console.info(`\n${signal} received. Closing DB pool...`);
  try {
    await pgPool.end();
    console.info('✅ PostgreSQL pool closed. Bye!');
    process.exit(0);
  } catch (e) {
    console.error('❌ Error closing pool:', e.message);
    process.exit(1);
  }
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
