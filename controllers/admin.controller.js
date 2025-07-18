const db = require('../config/db');
const logger = require('../utils/logger');
const bcrypt = require('bcrypt');

exports.getUsersFromTokenCompany = async (req, res) => {
  try {
    const { company_id, role } = req.user;

    if (!['Admin', 'Super Admin'].includes(role)) {
      logger.warn(`[ACCESS DENIED] User with role ${role} attempted to fetch all users`);
      return res.status(403).json({ message: 'Access denied: Insufficient permissions' });
    }

    const result = await db.query(`
      SELECT 
        id, username, first_name, last_name, email, contact_number,
        designation, user_type, verified, is_online, blocked,
        company_id, zone_id, created_at
      FROM senso.senso_users
      WHERE company_id = $1
      ORDER BY created_at DESC
    `, [company_id]);

    logger.info(`[USER FETCH] ${result.rowCount} users fetched for company ${company_id}`);

    res.status(200).json({
      message: 'Users fetched successfully',
      users: result.rows
    });
  } catch (err) {
    logger.error(`[USER FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch users', error: 'Internal Server Error' });
  }
};

exports.addUser = async (req, res) => {
  try {
    const { company_id, role, zone_id: admin_zone_id } = req.user;
    const {
      first_name, last_name, email, contact_number,
      designation, user_type, password, zone_id
    } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!email || !password || !first_name || !last_name || !user_type) {
      return res.status(400).json({ message: 'Missing required fields' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await db.query(`
      INSERT INTO senso.senso_users 
        (username, first_name, last_name, email, password, contact_number,
         designation, user_type, verified, blocked, company_id, zone_id)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, false, $9, $10)
      RETURNING id, email, user_type, first_name, last_name
    `, [
      email, first_name, last_name, email, hashedPassword,
      contact_number || '', designation || '', user_type,
      company_id, zone_id || admin_zone_id
    ]);

    res.status(201).json({ message: 'User added successfully', user: result.rows[0] });
  } catch (err) {
    logger.error(`[ADD USER ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add user', error: 'Internal Server Error' });
  }
};

exports.updateUser = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const {
      id, first_name, last_name, contact_number,
      designation, user_type, zone_id, blocked
    } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!id) {
      return res.status(400).json({ message: 'User ID is required' });
    }

    const result = await db.query(`
      UPDATE senso.senso_users SET
        first_name = $1,
        last_name = $2,
        contact_number = $3,
        designation = $4,
        user_type = $5,
        zone_id = $6,
        blocked = $7
      WHERE id = $8 AND company_id = $9
      RETURNING id, email, user_type, first_name, last_name
    `, [
      first_name, last_name, contact_number, designation,
      user_type, zone_id, blocked || false, id, company_id
    ]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found or unauthorized' });
    }

    res.status(200).json({ message: 'User updated successfully', user: result.rows[0] });
  } catch (err) {
    logger.error(`[UPDATE USER ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update user', error: 'Internal Server Error' });
  }
};

exports.deleteUser = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const result = await db.query(`
      DELETE FROM senso.senso_users
      WHERE id = $1 AND company_id = $2
      RETURNING id
    `, [id, company_id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found or unauthorized' });
    }

    res.status(200).json({ message: 'User deleted successfully' });
  } catch (err) {
    logger.error(`[DELETE USER ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete user', error: 'Internal Server Error' });
  }
};

exports.getZonesFromTokenCompany = async (req, res) => {
  try {
    const { company_id } = req.user;

    const result = await db.query(`
      SELECT 
        id, zone_name, created_at
      FROM senso.senso_zones
      WHERE company_id = $1
      ORDER BY created_at DESC
    `, [company_id]);

    res.status(200).json({
      message: 'Zones fetched successfully',
      zones: result.rows
    });
  } catch (err) {
    logger.error(`[ZONE FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch zones', error: 'Internal Server Error' });
  }
};

exports.addZone = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { zone_name } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!zone_name) {
      return res.status(400).json({ message: 'Zone name is required' });
    }

    const result = await db.query(`
      INSERT INTO senso.senso_zones (zone_name, company_id)
      VALUES ($1, $2)
      RETURNING id, zone_name, created_at
    `, [zone_name, company_id]);

    res.status(201).json({ message: 'Zone added successfully', zone: result.rows[0] });
  } catch (err) {
    logger.error(`[ADD ZONE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add zone', error: 'Internal Server Error' });
  }
};

exports.updateZone = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { id, zone_name } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!id || !zone_name) {
      return res.status(400).json({ message: 'Zone ID and new name are required' });
    }

    const result = await db.query(`
      UPDATE senso.senso_zones
      SET zone_name = $1
      WHERE id = $2 AND company_id = $3
      RETURNING id, zone_name, created_at
    `, [zone_name, id, company_id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Zone not found or unauthorized' });
    }

    res.status(200).json({ message: 'Zone updated successfully', zone: result.rows[0] });
  } catch (err) {
    logger.error(`[UPDATE ZONE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update zone', error: 'Internal Server Error' });
  }
};

exports.deleteZone = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const result = await db.query(`
      DELETE FROM senso.senso_zones
      WHERE id = $1 AND company_id = $2
      RETURNING id
    `, [id, company_id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Zone not found or unauthorized' });
    }

    res.status(200).json({ message: 'Zone deleted successfully' });
  } catch (err) {
    logger.error(`[DELETE ZONE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete zone', error: 'Internal Server Error' });
  }
};

exports.getDeviceTypes = async (req, res) => {
  try {
    const result = await db.query(`
      SELECT id, type_name, description, created_at
      FROM senso.senso_device_types
      ORDER BY created_at DESC
    `);

    res.status(200).json({ message: 'Device types fetched successfully', deviceTypes: result.rows });
  } catch (err) {
    logger.error(`[DEVICE TYPE FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch device types', error: 'Internal Server Error' });
  }
};

exports.addDeviceType = async (req, res) => {
  try {
    const { role } = req.user;
    const { type_name, description } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }
    if (!type_name) {
      return res.status(400).json({ message: 'Device type name is required' });
    }

    const result = await db.query(`
      INSERT INTO senso.senso_device_types (type_name, description)
      VALUES ($1, $2)
      RETURNING id, type_name, description, created_at
    `, [type_name, description || null]);

    res.status(201).json({ message: 'Device type added successfully', deviceType: result.rows[0] });
  } catch (err) {
    logger.error(`[ADD DEVICE TYPE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add device type', error: 'Internal Server Error' });
  }
};

exports.updateDeviceType = async (req, res) => {
  try {
    const { role } = req.user;
    const { id, type_name, description } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }
    if (!id || !type_name) {
      return res.status(400).json({ message: 'Device type ID and name are required' });
    }

    const result = await db.query(`
      UPDATE senso.senso_device_types
      SET type_name = $1, description = $2
      WHERE id = $3
      RETURNING id, type_name, description, created_at
    `, [type_name, description || null, id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Device type not found' });
    }

    res.status(200).json({
      message: 'Device type updated successfully',
      deviceType: result.rows[0]
    });
  } catch (err) {
    logger.error(`[UPDATE DEVICE TYPE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update device type', error: 'Internal Server Error' });
  }
};

exports.deleteDeviceType = async (req, res) => {
  try {
    const { role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const result = await db.query(`
      DELETE FROM senso.senso_device_types
      WHERE id = $1
      RETURNING id
    `, [id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Device type not found' });
    }

    res.status(200).json({ message: 'Device type deleted successfully' });
  } catch (err) {
    logger.error(`[DELETE DEVICE TYPE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete device type', error: 'Internal Server Error' });
  }
};

exports.getDevicesFromTokenCompany = async (req, res) => {
  try {
    const { company_id } = req.user;

    const result = await db.query(
      `
      SELECT 
        d.id,
        d.device_uid,
        d.device_name,
        d.status,
        d.issue_date,
        d.device_type_id,
        d.zone_id,
        JSON_BUILD_OBJECT(
          'id', dt.id,
          'name', COALESCE(dt.type_name, '')
        ) AS device_type,
        JSON_BUILD_OBJECT(
          'id', z.id,
          'name', COALESCE(z.zone_name, '')
        ) AS zone,
        JSON_BUILD_OBJECT(
          'id', w.id,
          'name', COALESCE(w.widget_name, ''),
          'unit', COALESCE(w.unit, ''),
          'width', w.width,
          'length', w.length,
          'diameter', w.diameter,
          'tank_type', w.tank_type
        ) AS widget
      FROM senso.senso_devices d
      LEFT JOIN senso.senso_device_types dt ON dt.id = d.device_type_id
      LEFT JOIN senso.senso_zones z ON z.id = d.zone_id
      LEFT JOIN senso.senso_device_widgets w ON w.id = d.widget_id
      WHERE d.company_id = $1
      ORDER BY d.issue_date DESC
      `,
      [company_id]
    );

    res.status(200).json({
      message: 'Devices fetched successfully',
      count: result.rowCount,
      devices: result.rows
    });
  } catch (err) {
    logger.error(`[DEVICE FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch devices', error: 'Internal Server Error' });
  }
};

exports.addDevice = async (req, res) => {
  try {
    const { company_id, role, zone_id: admin_zone_id } = req.user;
    const { device_uid, device_name, device_type_id, zone_id, widget_id } = req.body;
    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }
    if (!device_uid || !device_type_id) {
      return res.status(400).json({ message: 'Device UID and type are required' });
    }

    const zoneToCheck = zone_id || admin_zone_id;

    const validation = await db.query(
      `
      SELECT
        (SELECT COUNT(*) FROM senso.senso_devices WHERE device_uid = $1) AS device_exists,
        (SELECT COUNT(*) FROM senso.senso_device_types WHERE id = $2) AS type_exists,
        (SELECT COUNT(*) FROM senso.senso_zones WHERE id = $3 AND company_id = $4) AS zone_exists,
        (SELECT COUNT(*) FROM senso.senso_device_widgets WHERE id = $5) AS widget_exists
      `,
      [device_uid, device_type_id, zoneToCheck, company_id, widget_id || null]
    );

    const { device_exists, type_exists, zone_exists, widget_exists } = validation.rows[0];

    if (device_exists > 0) return res.status(409).json({ message: 'Device UID already exists' });
    if (type_exists === 0) return res.status(400).json({ message: 'Invalid device type ID' });
    if (zone_exists === 0) return res.status(400).json({ message: 'Invalid or unauthorized zone ID' });
    if (widget_id && widget_exists === 0) return res.status(400).json({ message: 'Invalid widget ID' });

    const result = await db.query(
      `
      INSERT INTO senso.senso_devices 
        (device_uid, device_name, company_id, zone_id, device_type_id, status, widget_id)
      VALUES ($1, $2, $3, $4, $5, 'offline', $6)
      RETURNING id, device_uid, device_name, device_type_id, zone_id, widget_id
      `,
      [device_uid, device_name || '', company_id, zoneToCheck, device_type_id, widget_id || null]
    );

    res.status(201).json({ message: 'Device added successfully', device: result.rows[0] });
  } catch (err) {
    logger.error(`[ADD DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add device', error: 'Internal Server Error' });
  }
};

exports.updateDevice = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { device_uid, device_name, device_type_id, zone_id, widget_id } = req.body;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!id) {
      return res.status(400).json({ message: 'Device ID is required' });
    }

    const validation = await db.query(
      `
      SELECT
        (SELECT COUNT(*) FROM senso.senso_devices WHERE id = $1 AND company_id = $5) AS device_exists,
        (SELECT COUNT(*) FROM senso.senso_device_types WHERE id = $2) AS type_exists,
        (SELECT COUNT(*) FROM senso.senso_zones WHERE id = $3 AND company_id = $5) AS zone_exists,
        (SELECT COUNT(*) FROM senso.senso_device_widgets WHERE id = $4) AS widget_exists
      `,
      [id, device_type_id, zone_id, widget_id || null, company_id]
    );

    const { device_exists, type_exists, zone_exists, widget_exists } = validation.rows[0];

    if (device_exists === 0) return res.status(404).json({ message: 'Device not found or unauthorized' });
    if (type_exists === 0) return res.status(400).json({ message: 'Invalid device type ID' });
    if (zone_exists === 0) return res.status(400).json({ message: 'Invalid or unauthorized zone ID' });
    if (widget_id && widget_exists === 0) return res.status(400).json({ message: 'Invalid widget ID' });

    const result = await db.query(
      `
      UPDATE senso.senso_devices
      SET device_uid = $1, device_name = $2, device_type_id = $3, zone_id = $4, widget_id = $5
      WHERE id = $6 AND company_id = $7
      RETURNING id, device_uid, device_name, device_type_id, zone_id, widget_id
      `,
      [device_uid, device_name || '', device_type_id, zone_id, widget_id || null, id, company_id]
    );

    res.status(200).json({ message: 'Device updated successfully', device: result.rows[0] });
  } catch (err) {
    logger.error(`[UPDATE DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update device', error: 'Internal Server Error' });
  }
};


exports.deleteDevice = async (req, res) => {
  try {
    const { company_id, role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const validation = await db.query(
      `SELECT COUNT(*) AS device_exists FROM senso.senso_devices WHERE id = $1 AND company_id = $2`,
      [id, company_id]
    );

    if (validation.rows[0].device_exists === 0) {
      return res.status(404).json({ message: 'Device not found or unauthorized' });
    }

    await db.query(`DELETE FROM senso.senso_devices WHERE id = $1 AND company_id = $2`, [id, company_id]);
    res.status(200).json({ message: 'Device deleted successfully' });
  } catch (err) {
    logger.error(`[DELETE DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete device', error: 'Internal Server Error' });
  }
};

exports.getWidgetsByDevice = async (req, res) => {
  try {
    const { device_id } = req.params;

    const result = await db.query(`
      SELECT 
        id, widget_name, unit, width, length, diameter, tank_type, created_at
      FROM senso.senso_device_widgets
      WHERE device_type_id = $1
      ORDER BY created_at DESC
    `, [device_id]);

    res.status(200).json({ message: 'Widgets fetched successfully', widgets: result.rows });
  } catch (err) {
    logger.error(`[WIDGET FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch widgets', error: 'Internal Server Error' });
  }
};

exports.addWidget = async (req, res) => {
  try {
    const { role } = req.user;
    const { device_type_id, widget_name, unit, width, length, diameter, tank_type } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }
    if (!device_type_id || !widget_name) {
      return res.status(400).json({ message: 'Device Type ID and widget name are required' });
    }

    const typeCheck = await db.query(
      `SELECT id FROM senso.senso_device_types WHERE id = $1`, [device_type_id]
    );
    if (typeCheck.rowCount === 0) {
      return res.status(400).json({ message: 'Invalid device_type_id: Type does not exist' });
    }

    const safeWidth = (width === null || width === undefined || width === '') ? null : Number(width);
    const safeLength = (length === null || length === undefined || length === '') ? null : Number(length);
    const safeDiameter = (diameter === null || diameter === undefined || diameter === '') ? null : Number(diameter);

    const result = await db.query(`
      INSERT INTO senso.senso_device_widgets
        (device_type_id, widget_name, unit, width, length, diameter, tank_type)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id, device_type_id, widget_name, unit, width, length, diameter, tank_type, created_at
    `, [
      device_type_id,
      widget_name,
      unit || '',
      safeWidth,
      safeLength,
      safeDiameter,
      tank_type || null
    ]);

    res.status(201).json({ message: 'Widget added successfully', widget: result.rows[0] });

  } catch (err) {
    logger.error(`[ADD WIDGET ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add widget', error: err.message });
  }
};

exports.updateWidget = async (req, res) => {
  try {
    const { role } = req.user;
    const { id, widget_name, unit, width, length, diameter, tank_type, device_type_id } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }
    if (!id) {
      return res.status(400).json({ message: 'Widget ID is required' });
    }
    if (!device_type_id) {
      return res.status(400).json({ message: 'Device Type ID is required' });
    }

    const typeCheck = await db.query(
      `SELECT id FROM senso.senso_device_types WHERE id = $1`, [device_type_id]
    );
    if (typeCheck.rowCount === 0) {
      return res.status(400).json({ message: 'Invalid device_type_id: Type does not exist' });
    }

    const safeWidth = (width === null || width === undefined || width === '') ? null : Number(width);
    const safeLength = (length === null || length === undefined || length === '') ? null : Number(length);
    const safeDiameter = (diameter === null || diameter === undefined || diameter === '') ? null : Number(diameter);

    const result = await db.query(`
      UPDATE senso.senso_device_widgets
      SET widget_name = $1, unit = $2, width = $3, length = $4, diameter = $5, tank_type = $6, device_type_id = $7
      WHERE id = $8
      RETURNING id, device_type_id, widget_name, unit, width, length, diameter, tank_type
    `, [
      widget_name,
      unit,
      safeWidth,
      safeLength,
      safeDiameter,
      tank_type,
      device_type_id,
      id
    ]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Widget not found' });
    }

    res.status(200).json({
      message: 'Widget updated successfully',
      widget: result.rows[0]
    });
  } catch (err) {
    logger.error(`[UPDATE WIDGET ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update widget', error: err.message });
  }
};

exports.deleteWidget = async (req, res) => {
  try {
    const { role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const result = await db.query(`
      DELETE FROM senso.senso_device_widgets
      WHERE id = $1
      RETURNING id
    `, [id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Widget not found' });
    }

    res.status(200).json({ message: 'Widget deleted successfully' });
  } catch (err) {
    logger.error(`[DELETE WIDGET ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete widget', error: 'Internal Server Error' });
  }
};

exports.getDeviceDataWithBucketing = async (req, res) => {
  try {
    console.time("TOTAL_API_TIME");
    const { company_id } = req.user;
    const { deviceuid, start_date, end_date } = req.query;

    if (!deviceuid || !start_date || !end_date) {
      return res.status(400).json({ message: 'deviceuid, start_date, and end_date are required' });
    }

    console.time("DEVICE_CHECK");
    const deviceCheck = await db.query(
      `SELECT id FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2`,
      [deviceuid, company_id]
    );
    console.timeEnd("DEVICE_CHECK");

    if (deviceCheck.rowCount === 0) {
      return res.status(403).json({ message: 'Unauthorized device access' });
    }

    const start = new Date(start_date);
    const end = new Date(end_date);
    const diffHours = (end - start) / (1000 * 60 * 60);

    let intervalMinutes = 1;
    if (diffHours <= 1) intervalMinutes = 1;
    else if (diffHours <= 12) intervalMinutes = 5;
    else if (diffHours <= 24) intervalMinutes = 5;
    else if (diffHours <= 168) intervalMinutes = 30;
    else if (diffHours <= 720) intervalMinutes = 60;
    else intervalMinutes = 120;

    console.time("DB_QUERY");
    const result = await db.query(
      `
      SELECT
        TO_CHAR(
          date_trunc('hour', "timestamp")
          + floor((EXTRACT(minute FROM "timestamp") / $1)) * ($1 * interval '1 minute'),
          'YYYY-MM-DD"T"HH24:MI:SS"Z"'
        ) AS bucket_time,
        ROUND(AVG(temperature)::numeric, 2) AS temperature,
        ROUND(AVG(humidity)::numeric, 2) AS humidity,
        ROUND(AVG(temperaturer)::numeric, 2) AS temperaturer,
        ROUND(AVG(temperaturey)::numeric, 2) AS temperaturey,
        ROUND(AVG(temperatureb)::numeric, 2) AS temperatureb,
        ROUND(AVG(pressure)::numeric, 2) AS pressure,
        ROUND(AVG(flowrate)::numeric, 2) AS flowrate,
        MAX(totalvolume) AS totalvolume
      FROM senso.senso_data
      WHERE deviceuid = $2
        AND "timestamp" BETWEEN $3 AND $4
      GROUP BY bucket_time
      ORDER BY bucket_time ASC
      `,
      [intervalMinutes, deviceuid, start_date, end_date]
    );
    console.timeEnd("DB_QUERY");

    console.log("Rows fetched:", result.rowCount);

    res.status(200).json({
      message: 'Data fetched successfully',
      bucket_interval: `${intervalMinutes} minutes`,
      count: result.rowCount,
      data: result.rows
    });

    console.timeEnd("TOTAL_API_TIME");
  } catch (err) {
    logger.error(`[DATA FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch data', error: err.message });
  }
};


exports.getLatestTwoEntriesPerDevice = async (req, res) => {
  try {
    const { company_id } = req.user;

    const result = await db.query(
      `
      SELECT d.device_uid, ld.temperature, ld.humidity, ld.temp_r, ld.temp_y, ld.temp_b,
             ld.pressure, ld.flowrate, ld.totalvolume, ld.timestamp
      FROM senso.senso_devices d
      JOIN LATERAL (
        SELECT 
          temperature,
          humidity,
          temperaturer AS temp_r,
          temperaturey AS temp_y,
          temperatureb AS temp_b,
          pressure,
          flowrate,
          totalvolume,
          timestamp
        FROM senso.senso_data sd
        WHERE sd.deviceuid = d.device_uid
        ORDER BY timestamp DESC
        LIMIT 2
      ) ld ON TRUE
      WHERE d.company_id = $1
      ORDER BY d.device_uid, ld.timestamp DESC
      `,
      [company_id]
    );

    // Group into Latest & Previous
    const grouped = {};
    result.rows.forEach((row) => {
      if (!grouped[row.device_uid]) {
        grouped[row.device_uid] = { DeviceUID: row.device_uid };
      }
      const mapped = {
        Temperature: row.temperature,
        Humidity: row.humidity,
        Temp_R: row.temp_r,
        Temp_Y: row.temp_y,
        Temp_B: row.temp_b,
        Pressure: row.pressure,
        FlowRate: row.flowrate,
        TotalVolume: row.totalvolume,
        UpdatedAt: row.timestamp
      };

      if (!grouped[row.device_uid].Latest) {
        grouped[row.device_uid].Latest = mapped;
      } else {
        grouped[row.device_uid].Previous = Object.fromEntries(
          Object.entries(mapped).map(([k, v]) =>
            k === 'UpdatedAt' ? [k, v] : [`Previous${k}`, v]
          )
        );
      }
    });

    res.status(200).json({
      message: 'Latest two entries fetched successfully',
      count: Object.keys(grouped).length,
      data: Object.values(grouped)
    });
  } catch (err) {
    logger.error(`[LATEST TWO ENTRIES ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch latest two entries', error: err.message });
  }
};

exports.getDeviceDataWithBucket = async (req, res) => {
  try {
    const { company_id } = req.user;
    const { deviceuid, start_date, end_date, bucket } = req.query;

    if (!deviceuid || !start_date || !end_date || !bucket) {
      return res.status(400).json({ message: 'deviceuid, start_date, end_date, and bucket are required' });
    }

    const allowedBuckets = [
      5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60,
      120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720,
      'day'
    ];

    let parsedBucket = bucket === 'day' ? 'day' : parseInt(bucket, 10);

    if (parsedBucket !== 'day' && (isNaN(parsedBucket) || parsedBucket <= 0)) {
      return res.status(400).json({ message: 'Invalid bucket: must be positive integer or "day"' });
    }

    if (!allowedBuckets.includes(parsedBucket) && parsedBucket !== 'day') {
      console.warn(`[CUSTOM BUCKET] Using custom bucket size: ${parsedBucket} minutes`);
    }

    const deviceCheck = await db.query(
      `SELECT id FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2`,
      [deviceuid, company_id]
    );

    if (deviceCheck.rowCount === 0) {
      return res.status(403).json({ message: 'Unauthorized device access' });
    }

    let bucketSql, bucketLabel, params;
    if (parsedBucket === 'day') {
      bucketSql = `date_trunc('day', "timestamp")`;
      bucketLabel = '1 day';
      params = [deviceuid, start_date, end_date];
    } else {
      bucketSql = `
        date_trunc('hour', "timestamp")
        + floor((EXTRACT(epoch FROM "timestamp") / ($1 * 60))) * ($1 * interval '1 minute')
      `;
      bucketLabel = `${parsedBucket} minutes`;
      params = [parsedBucket, deviceuid, start_date, end_date];
    }

    const result = await db.query(
      `
      SELECT 
        TO_CHAR(
          ${bucketSql},
          'YYYY-MM-DD"T"HH24:MI:SS"Z"'
        ) AS bucket_time,
        ROUND(AVG(temperature)::numeric, 2) AS temperature,
        ROUND(AVG(humidity)::numeric, 2) AS humidity,
        ROUND(AVG(temperaturer)::numeric, 2) AS temperaturer,
        ROUND(AVG(temperaturey)::numeric, 2) AS temperaturey,
        ROUND(AVG(temperatureb)::numeric, 2) AS temperatureb,
        ROUND(AVG(pressure)::numeric, 2) AS pressure,
        ROUND(AVG(flowrate)::numeric, 2) AS flowrate,
        MAX(totalvolume) AS totalvolume
      FROM senso.senso_data
      WHERE deviceuid = $2
        AND "timestamp" BETWEEN $3 AND $4
      GROUP BY bucket_time
      ORDER BY bucket_time ASC
      `,
      params
    );

    res.status(200).json({
      message: 'Data fetched successfully',
      bucket_interval: bucketLabel,
      count: result.rowCount,
      data: result.rows
    });
  } catch (err) {
    logger.error(`[BUCKET DATA FETCH ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch data', error: err.message });
  }
};


