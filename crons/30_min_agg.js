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
  const query = `
    INSERT INTO senso.senso_data_30_min (
      deviceuid, "timestamp", temperature, humidity, flowrate,
      temperaturer, temperaturey, temperatureb, pressure, totalvolume
    )
    SELECT
      deviceuid,
      DATE_TRUNC('minute', "timestamp") - 
        ((EXTRACT(MINUTE FROM "timestamp")::int % 30) * INTERVAL '1 minute') AS bucket_start_time,
      ROUND(AVG(temperature)::numeric, 1),
      ROUND(AVG(humidity)::numeric, 1),
      ROUND(AVG(flowrate)::numeric, 1),
      ROUND(AVG(temperaturer)::numeric, 1),
      ROUND(AVG(temperaturey)::numeric, 1),
      ROUND(AVG(temperatureb)::numeric, 1),
      ROUND(AVG(pressure)::numeric, 1),
      MAX(totalvolume)
    FROM senso.senso_data
    WHERE "timestamp" >= NOW() - INTERVAL '30 minutes'
      AND deviceuid = ANY($1)
    GROUP BY deviceuid, bucket_start_time
    ON CONFLICT (deviceuid, "timestamp")
    DO UPDATE SET
      temperature = EXCLUDED.temperature,
      humidity = EXCLUDED.humidity,
      flowrate = EXCLUDED.flowrate,
      temperaturer = EXCLUDED.temperaturer,
      temperaturey = EXCLUDED.temperaturey,
      temperatureb = EXCLUDED.temperatureb,
      pressure = EXCLUDED.pressure,
      totalvolume = EXCLUDED.totalvolume;
  `;
  const res = await pgPool.query(query, [deviceBatch]);
  console.info(
    `[${new Date().toISOString()}] ✅ Batch processed: ${deviceBatch.length} devices, ${res.rowCount} rows affected`
  );
}

async function insertCleanData() {
  try {
    const devices = await getDistinctDevices();
    if (!devices.length) {
      console.warn(`[${new Date().toISOString()}] ⚠ No active devices in last 30 minutes`);
      return;
    }
    console.info(`[${new Date().toISOString()}] Found ${devices.length} active devices`);
    for (let i = 0; i < devices.length; i += BATCH_SIZE) {
      const batch = devices.slice(i, i + BATCH_SIZE);
      await insertBatch(batch);
    }
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ❌ Aggregation Error:`, err.message);
  }
}

cron.schedule('*/30 * * * *', insertCleanData);
console.info('✅ Cron initialized: running every 30 minutes');
