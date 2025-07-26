const db = require('../config/db');
const logger = require('../utils/logger');
const cron = require('node-cron');

cron.schedule('*/10 * * * * *', async () => {
  try {
    logger.info('[CRON] Running optimized device status updater...');
    await db.query(`
      WITH latest_data AS (
        SELECT 
          d.device_uid,
          MAX(s.timestamp)::timestamp AS latest_timestamp
        FROM senso.senso_devices d
        LEFT JOIN senso.senso_data s 
          ON s.deviceuid = d.device_uid
        GROUP BY d.device_uid
      )
      UPDATE senso.senso_devices AS d
      SET status = CASE
            WHEN latest_data.latest_timestamp IS NOT NULL
                 AND latest_data.latest_timestamp >= NOW() - INTERVAL '30 minutes'
              THEN 'online'
            ELSE 'offline'
          END
      FROM latest_data
      WHERE d.device_uid = latest_data.device_uid
    `);
    logger.info('[CRON] ✅ Device statuses updated successfully');
  } catch (err) {
    logger.error(`[CRON ERROR] ❌ ${err.stack}`);
  }
});
