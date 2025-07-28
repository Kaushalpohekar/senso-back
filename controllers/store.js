const mqtt = require('mqtt');
const { Pool } = require('pg');
const os = require('os');

const broker = 'ws://dashboard.senselive.in:9001';
const pgPool = new Pool({
  host: 'data.senselive.in',
  user: 'senselive',
  password: 'SenseLive@2025',
  database: 'senselive_db',
  port: 5432,
  ssl: { rejectUnauthorized: false },
  max: 100
});

function getLocalIpAddress() {
  const interfaces = os.networkInterfaces();
  for (const key in interfaces) {
    for (const item of interfaces[key]) {
      if (item.family === 'IPv4' && !item.internal) return item.address;
    }
  }
  return '0.0.0.0';
}

function normalizeKey(key) {
  return key.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function getUniversalValue(data, names) {
  const normalizedData = {};
  for (const key in data) {
    if (typeof data[key] === 'object' && data[key] !== null && !Array.isArray(data[key])) continue;
    normalizedData[normalizeKey(key)] = data[key];
  }
  for (const n of names) {
    const norm = normalizeKey(n);
    if (norm in normalizedData) return normalizedData[norm];
  }
  return null;
}

const localSystemIp = getLocalIpAddress();
const insertQueue = [];
const insertTracker = new Map();
let isFlushing = false;

function shouldInsertNow(deviceUID) {
  const now = Date.now();
  if (!insertTracker.has(deviceUID)) {
    insertTracker.set(deviceUID, { lastInserted: now });
    return true;
  }

  const record = insertTracker.get(deviceUID);
  if (now - record.lastInserted >= 60000) {
    record.lastInserted = now;
    insertTracker.set(deviceUID, record);
    return true;
  }

  return false;
}

async function flushInsertQueue() {
  if (isFlushing || insertQueue.length === 0) return;
  isFlushing = true;
  const batch = insertQueue.splice(0, insertQueue.length);

  const values = [];
  const placeholders = [];
  batch.forEach((item, i) => {
    const offset = i * 11;
    placeholders.push(`($${offset + 1}, NOW(), $${offset + 2}, $${offset + 3}, $${offset + 4},
                       $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8},
                       $${offset + 9}, $${offset + 10}, $${offset + 11})`);
    values.push(...item.values);
  });

  try {
    await pgPool.query(
      `INSERT INTO senso.senso_data (
        deviceuid, "timestamp", temperature, temperaturer, temperaturey,
        temperatureb, humidity, flowrate, pressure, totalvolume, ip_address, status
      ) VALUES ${placeholders.join(",")}`,
      values
    );
    console.log(`✅ [${new Date().toISOString()}] Inserted ${batch.length} rows`);
  } catch (err) {
    console.error(`❌ Batch Insert Error: ${err.message}`);
  }

  isFlushing = false;
}

setInterval(flushInsertQueue, 1000);

async function updateDeviceStatus(deviceUID, status) {
  try {
    await pgPool.query(
      `UPDATE senso.senso_devices SET status = $1 WHERE device_uid = $2`,
      [status, deviceUID]
    );
    console.log(`📶 [${new Date().toISOString()}] ${deviceUID} marked ${status}`);
  } catch (err) {
    console.error(`❌ Status Update Error: ${err.message}`);
  }
}

const mqttClient = mqtt.connect(broker, {
  username: 'Sense2023',
  password: 'sense123',
  reconnectPeriod: 5000
});

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe(['Sense/#', 'Elkem/Water/#'], (err) => {
    if (err) console.error('Subscription error:', err.message);
  });
});

mqttClient.on('message', async (topic, message) => {
  try {
    const payload = JSON.parse(message);
    const devices = [];

    if (payload.DeviceUID) devices.push(payload);
    for (const key in payload) {
      const val = payload[key];
      if (typeof val === 'object' && val?.DeviceUID && (val.FlowRate !== undefined || val.Totalizer !== undefined)) {
        devices.push(val);
      }
    }

    for (const data of devices) {
      const deviceUID = getUniversalValue(data, ['DeviceUID']);
      if (!deviceUID) continue;

      if (shouldInsertNow(deviceUID)) {
        const insertValues = [
          deviceUID.trim(),
          getUniversalValue(data, ['Temperature']),
          getUniversalValue(data, ['TemperatureR', 'Temp1']),
          getUniversalValue(data, ['TemperatureY', 'Temp2']),
          getUniversalValue(data, ['TemperatureB', 'Temp3']),
          getUniversalValue(data, ['Humidity']),
          getUniversalValue(data, ['FlowRate', 'flow_rate', 'Level', 'level']),
          getUniversalValue(data, ['Pressure']),
          getUniversalValue(data, ['Totalizer', 'TotalVolume']),
          data.LocalIP || localSystemIp,
          'online'
        ];
        insertQueue.push({ values: insertValues });
        await updateDeviceStatus(deviceUID, 'online');
      }
    }
  } catch (err) {
    console.error(`❌ Handler Error: ${err.message}`);
  }
});

mqttClient.on('error', (err) => console.error('MQTT error:', err.message));
process.on('exit', async () => await pgPool.end());
