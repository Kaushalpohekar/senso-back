const { Pool } = require("pg");
const mqtt = require("mqtt");
const nodemailer = require("nodemailer");
const ejs = require("ejs");
const fs = require("fs");
const path = require("path");

const MQTT_BROKER = "ws://dashboard.senselive.in:9001";
const MQTT_USERNAME = "Sense2023";
const MQTT_PASSWORD = "sense123";

let deviceMap = new Map();
let lastSentMap = new Map();
const logQueue = [];

const db = new Pool({
    host: "data.senselive.in",
    user: "senselive",
    password: "SenseLive@2025",
    database: "senselive_db",
    port: 5432,
    ssl: { rejectUnauthorized: false },
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000
});

const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: {
        user: 'donotreplysenselive@gmail.com',
        pass: 'xgcklimtlbswtzfq',
    },
});

const thresholdAliases = {
    temp: "temperature",
    temperature: "temperature",
    tempr: "temperaturer",
    temperaturer: "temperaturer",
    tempy: "temperaturey",
    temperaturey: "temperaturey",
    tempb: "temperatureb",
    temperatureb: "temperatureb",
    hum: "humidity",
    humidity: "humidity",
    flow: "flowrate",
    level: "flowrate",
    flowrate: "flowrate",
    totalvolume: "totalizer",
    totalizer: "totalizer"
};

async function fetchDeviceData() {
    try {
        const result = await db.query(`
            SELECT 
                d.id AS device_id,
                d.device_uid,
                d.device_name,
                dt.type_name AS device_type,
                n.id,
                n.threshold_type,
                n.condition,
                n.threshold_value,
                n.interval_minutes,
                n.notify_email,
                n.notify_whatsapp,
                n.notify_sms,
                n.user_id
            FROM senso.senso_device_notifications n
            JOIN senso.senso_devices d ON n.device_id = d.id
            JOIN senso.senso_device_types dt ON d.device_type_id = dt.id
            WHERE n.enabled = true
        `);

        deviceMap.clear();

        for (const row of result.rows) {
            const {
                id, device_id, user_id, device_uid, device_name, device_type,
                threshold_type, condition, threshold_value,
                interval_minutes, notify_email, notify_whatsapp, notify_sms
            } = row;

            const entry = {
                id,
                device_id,
                user_id,
                device_name,
                device_type,
                threshold_type,
                condition,
                threshold_value: parseFloat(threshold_value),
                interval: interval_minutes || 10,
                notify_email,
                notify_whatsapp,
                notify_sms
            };

            if (!deviceMap.has(device_uid)) deviceMap.set(device_uid, []);
            deviceMap.get(device_uid).push(entry);
        }

        console.info(`[${new Date().toISOString()}] ✅ Device alert config updated (${deviceMap.size} devices)`);
    } catch (err) {
        console.error("❌ Error fetching device alert data:", err);
    }
}

function evaluateCondition(value, condition, threshold) {
    switch (condition) {
        case ">": return value > threshold;
        case ">=": return value >= threshold;
        case "<": return value < threshold;
        case "<=": return value <= threshold;
        case "==": return value == threshold;
        default: return false;
    }
}

async function processTrigger(deviceUID, payload) {
    if (!deviceMap.has(deviceUID)) return;

    const configs = deviceMap.get(deviceUID);
    const now = Date.now();
    const normalizedPayload = {};
    const logBatch = [];

    for (const key in payload) {
        if (typeof payload[key] === "number") {
            normalizedPayload[key.toLowerCase()] = payload[key];
        }
    }

    for (const config of configs) {
        const ruleId = config.id;
        let typeKey = config.threshold_type?.toLowerCase();
        typeKey = thresholdAliases[typeKey] || typeKey;

        let reading = null;

        for (const alias in thresholdAliases) {
            if (thresholdAliases[alias] === typeKey && normalizedPayload.hasOwnProperty(alias)) {
                reading = normalizedPayload[alias];
                break;
            }
        }

        if (reading === null || isNaN(reading)) continue;

        const lastSent = lastSentMap.get(ruleId) || 0;
        const intervalMs = config.interval * 60 * 1000;

        if ((now - lastSent) >= intervalMs && evaluateCondition(reading, config.condition, config.threshold_value)) {
            console.log(`📊 Alert triggered for ${deviceUID} (${config.device_name}): ${typeKey} ${config.condition} ${config.threshold_value}, current: ${reading}`);
            lastSentMap.set(ruleId, now);

            logBatch.push({
                device_id: config.device_id,
                user_id: config.user_id,
                input_name: config.threshold_type,
                triggered_value: reading,
                condition: config.condition,
                threshold: config.threshold_value,
                sent_to_email: config.notify_email ? "yes" : null,
                sent_to_whatsapp: config.notify_whatsapp ? "yes" : null,
                sent_to_sms: config.notify_sms ? "yes" : null,
                sent_status: "sent",
                action_taken: false
            });
        }
    }

    if (logBatch.length > 0) {
        logQueue.push(...logBatch);
    }
}

function handleIncomingPayload(payload) {
    if (payload.DeviceUID) {
        processTrigger(payload.DeviceUID, payload);
    } else {
        for (const key in payload) {
            if (key.startsWith("Meter_")) {
                const sub = payload[key];
                if (sub && sub.DeviceUID) {
                    processTrigger(sub.DeviceUID, sub);
                }
            }
        }
    }
}

async function insertNotificationLogsBatch(logs) {
    if (!logs.length) return;

    const query = `
        INSERT INTO senso.senso_notification_logs (
            device_id, user_id, input_name, triggered_value, condition, threshold,
            sent_to_email, sent_to_whatsapp, sent_to_sms, sent_status, action_taken
        ) VALUES 
        ${logs.map((_, i) => `(
            $${i * 11 + 1}, $${i * 11 + 2}, $${i * 11 + 3}, $${i * 11 + 4}, $${i * 11 + 5}, 
            $${i * 11 + 6}, $${i * 11 + 7}, $${i * 11 + 8}, $${i * 11 + 9}, $${i * 11 + 10}, $${i * 11 + 11}
        )`).join(', ')}
    `;

    const values = logs.flatMap(log => [
        log.device_id,
        log.user_id,
        log.input_name,
        log.triggered_value,
        log.condition,
        log.threshold,
        log.sent_to_email,
        log.sent_to_whatsapp,
        log.sent_to_sms,
        log.sent_status,
        log.action_taken
    ]);

    try {
        await db.query(query, values);
        console.log(`📝 Batch inserted ${logs.length} alert log(s)`);
    } catch (err) {
        console.error("❌ Batch insert failed:", err.message);
        console.error(err.stack);
    }
}

const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD
});

mqttClient.on("connect", () => {
    console.log("🔌 Connected to MQTT broker");
    mqttClient.subscribe("Sense/Live/#", err => {
        if (err) console.error("❌ MQTT Subscribe Error:", err);
    });
});

mqttClient.on("message", (topic, message) => {
    let payloadString = message.toString().trim();

    if (!payloadString.startsWith("{") || !payloadString.endsWith("}")) {
        console.warn(`⚠️ Skipping non-JSON MQTT message on topic "${topic}":`, payloadString);
        return;
    }

    try {
        const payload = JSON.parse(payloadString);
        handleIncomingPayload(payload);
        console.log(`📥 Received MQTT message on topic "${topic}"`);
    } catch (error) {
        console.error("❌ Invalid MQTT JSON:", error.message);
        console.error("🚫 Raw payload received:", payloadString);
    }
});

setInterval(fetchDeviceData, 10000);
fetchDeviceData();

setInterval(async () => {
    if (logQueue.length === 0) return;
    const logsToInsert = logQueue.splice(0, logQueue.length);
    await insertNotificationLogsBatch(logsToInsert);
}, 3000);