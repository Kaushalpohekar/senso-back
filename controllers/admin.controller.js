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
        ) AS widget,

        JSON_BUILD_OBJECT(
          'id', u.id,
          'name', CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, ''))
        ) AS assigned_to,

        COALESCE(
          (
            SELECT JSON_AGG(
              JSON_BUILD_OBJECT(
                'id', c.id,
                'parameter', COALESCE(c.parameter, ''),
                'custom_unit', COALESCE(c.custom_unit, ''),
                'length', c.length,
                'width', c.width,
                'height', c.height,
                'calc_operator', COALESCE(c.calc_operator, ''),
                'calc_operand', c.calc_operand,
                'calc_expression', COALESCE(c.calc_expression, ''),
                'tank_type', COALESCE(c.tank_type, ''),
                'is_active', c.is_active
              )
            )
            FROM senso.senso_device_calculation_config c
            WHERE c.device_id = d.id AND c.is_active = true
          ),
          '[]'::json
        ) AS calculation_configs,

        COALESCE(
          (
            SELECT JSON_AGG(
              JSON_BUILD_OBJECT(
                'id', n.id,
                'enabled', n.enabled,
                'threshold_type', COALESCE(n.threshold_type, ''),
                "condition", COALESCE(n."condition", ''),
                'threshold_value', n.threshold_value,
                'notify_email', n.notify_email,
                'notify_whatsapp', n.notify_whatsapp,
                'notify_sms', n.notify_sms
              )
            )
            FROM senso.senso_device_notifications n
            WHERE n.device_id = d.id
          ),
          '[]'::json
        ) AS notifications

      FROM senso.senso_devices d
      LEFT JOIN senso.senso_device_types dt ON dt.id = d.device_type_id
      LEFT JOIN senso.senso_zones z ON z.id = d.zone_id
      LEFT JOIN senso.senso_device_widgets w ON w.id = d.widget_id
      LEFT JOIN senso.senso_device_users du ON du.device_id = d.id
      LEFT JOIN senso.senso_users u ON u.id = du.user_id
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
  const client = await db.connect();
  try {
    const { company_id, role, zone_id: admin_zone_id } = req.user;
    const {
      device_uid,
      device_name,
      device_type_id,
      zone_id,
      widget_id,
      assignedTo,
      issueDate,
      calculations,
      tankType,
      length,
      width,
      height,
      diameter
    } = req.body;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!device_uid || !device_type_id || !assignedTo) {
      return res.status(400).json({ message: 'Device UID, device type, and assigned user ID are required' });
    }

    const zoneToCheck = zone_id || admin_zone_id;

    const validation = await client.query(
      `
      SELECT
        (SELECT COUNT(*) FROM senso.senso_devices WHERE device_uid = $1) AS device_exists,
        (SELECT COUNT(*) FROM senso.senso_device_types WHERE id = $2) AS type_exists,
        (SELECT COUNT(*) FROM senso.senso_zones WHERE id = $3 AND company_id = $5) AS zone_exists,
        (SELECT COUNT(*) FROM senso.senso_device_widgets WHERE id = $4) AS widget_exists,
        (SELECT user_type FROM senso.senso_users WHERE id = $6 AND company_id = $5) AS user_type
      `,
      [device_uid, device_type_id, zoneToCheck, widget_id || null, company_id, assignedTo]
    );

    const { device_exists, type_exists, zone_exists, widget_exists, user_type } = validation.rows[0];

    if (device_exists > 0) return res.status(409).json({ message: 'Device UID already exists' });
    if (type_exists === 0) return res.status(400).json({ message: 'Invalid device type ID' });
    if (zone_exists === 0) return res.status(400).json({ message: 'Invalid or unauthorized zone ID' });
    if (widget_id && widget_exists === 0) return res.status(400).json({ message: 'Invalid widget ID' });
    if (!user_type) return res.status(400).json({ message: 'Invalid or unauthorized user ID' });

    const accessType = user_type === 'Standard' ? 'read' : 'write';

    await client.query('BEGIN');

    const deviceResult = await client.query(
      `
      INSERT INTO senso.senso_devices 
        (device_uid, device_name, company_id, zone_id, device_type_id, status, widget_id, issue_date)
      VALUES ($1, $2, $3, $4, $5, 'offline', $6, $7)
      RETURNING id, device_uid, device_name, device_type_id, zone_id, widget_id
      `,
      [device_uid, device_name || '', company_id, zoneToCheck, device_type_id, widget_id || null, issueDate || new Date()]
    );

    const device = deviceResult.rows[0];

    await client.query(
      `
      INSERT INTO senso.senso_device_users (user_id, device_id, access_type)
      VALUES ($1, $2, $3)
      `,
      [assignedTo, device.id, accessType]
    );

    if (Array.isArray(calculations) && calculations.length > 0) {
      for (const calc of calculations) {
        await client.query(
          `
          INSERT INTO senso.senso_device_calculation_config
            (device_id, parameter, custom_unit, length, width, height, calc_operator, calc_operand, calc_expression, tank_type)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `,
          [
            device.id,
            calc.paramId,
            calc.unit || null,
            length ? Number(length).toFixed(2) : null,
            diameter ? Number(diameter).toFixed(2) : width ? Number(width).toFixed(2) : null,
            height ? Number(height).toFixed(2) : null,
            calc.calcOperator || null,
            calc.calcOperand !== undefined ? Number(calc.calcOperand).toFixed(2) : null,
            calc.calcExpression || null,
            tankType || null
          ]
        );
      }
    }

    await client.query('COMMIT');

    res.status(201).json({
      message: 'Device added successfully',
      device,
      assigned_user: { assignedTo, access_type: accessType }
    });
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error(`[ADD DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to add device', error: err.message });
  } finally {
    client.release();
  }
};

exports.updateDevice = async (req, res) => {
  const client = await db.connect();
  try {
    const { company_id, role } = req.user;
    const {
      device_uid,
      device_name,
      device_type_id,
      zone_id,
      widget_id,
      user_id,
      tankType,
      length,
      width,
      height,
      calculations
    } = req.body;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!id) {
      return res.status(400).json({ message: 'Device ID is required' });
    }

    const validation = await client.query(
      `
      SELECT
        (SELECT COUNT(*) FROM senso.senso_devices WHERE id = $1 AND company_id = $6) AS device_exists,
        (SELECT COUNT(*) FROM senso.senso_device_types WHERE id = $2) AS type_exists,
        (SELECT COUNT(*) FROM senso.senso_zones WHERE id = $3 AND company_id = $6) AS zone_exists,
        (SELECT COUNT(*) FROM senso.senso_device_widgets WHERE id = $4) AS widget_exists,
        (SELECT user_type FROM senso.senso_users WHERE id = $5 AND company_id = $6) AS user_type
      `,
      [id, device_type_id, zone_id, widget_id, user_id || null, company_id]
    );

    const { device_exists, type_exists, zone_exists, widget_exists, user_type } = validation.rows[0];

    if (device_exists === 0) return res.status(404).json({ message: 'Device not found or unauthorized' });
    if (type_exists === 0) return res.status(400).json({ message: 'Invalid device type ID' });
    if (zone_exists === 0) return res.status(400).json({ message: 'Invalid or unauthorized zone ID' });
    if (widget_exists === 0) return res.status(400).json({ message: 'Invalid widget ID' });
    if (user_id && !user_type) return res.status(400).json({ message: 'Invalid or unauthorized user ID' });

    const accessType = user_type === 'Standard' ? 'read' : 'write';

    await client.query('BEGIN');

    const deviceResult = await client.query(
      `
      UPDATE senso.senso_devices
      SET 
          device_uid = $1,
          device_name = $2,
          device_type_id = $3,
          zone_id = $4,
          widget_id = $5
      WHERE id = $6 AND company_id = $7
      RETURNING id, device_uid, device_name, device_type_id, zone_id, widget_id, issue_date
      `,
      [
        device_uid,
        device_name || '',
        device_type_id,
        zone_id,
        widget_id,
        id,
        company_id
      ]
    );

    if (user_id) {
      const checkUserDevice = await client.query(
        `SELECT COUNT(*) AS exists FROM senso.senso_device_users WHERE device_id = $1`,
        [id]
      );

      if (parseInt(checkUserDevice.rows[0].exists) > 0) {
        await client.query(
          `UPDATE senso.senso_device_users SET user_id = $1, access_type = $2 WHERE device_id = $3`,
          [user_id, accessType, id]
        );
      } else {
        await client.query(
          `INSERT INTO senso.senso_device_users (user_id, device_id, access_type) VALUES ($1, $2, $3)`,
          [user_id, id, accessType]
        );
      }
    }

    if (Array.isArray(calculations)) {
      await client.query(`DELETE FROM senso.senso_device_calculation_config WHERE device_id = $1`, [id]);

      for (const calc of calculations) {
        await client.query(
          `
          INSERT INTO senso.senso_device_calculation_config
            (device_id, parameter, custom_unit, length, width, height, calc_operator, calc_operand, calc_expression, tank_type)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `,
          [
            id,
            calc.paramId,
            calc.unit || null,
            calc.length ? Number(calc.length).toFixed(2) : length ? Number(length).toFixed(2) : null,
            calc.width ? Number(calc.width).toFixed(2) : width ? Number(width).toFixed(2) : null,
            calc.height ? Number(calc.height).toFixed(2) : height ? Number(height).toFixed(2) : null,
            calc.calcOperator || null,
            calc.calcOperand !== undefined ? Number(calc.calcOperand).toFixed(2) : 0,
            calc.calcExpression || `${calc.paramId} + 0`,
            calc.tankType || tankType || null
          ]
        );
      }
    }

    await client.query('COMMIT');

    res.status(200).json({
      message: 'Device updated successfully',
      device: deviceResult.rows[0],
      assigned_user: user_id ? { user_id, access_type: accessType } : null
    });
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error(`[UPDATE DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to update device', error: err.message });
  } finally {
    client.release();
  }
};



exports.deleteDevice = async (req, res) => {
  const client = await db.connect();
  try {
    const { company_id, role } = req.user;
    const { id } = req.params;

    if (!['Admin', 'Super Admin'].includes(role)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const validation = await client.query(
      `SELECT COUNT(*) AS device_exists FROM senso.senso_devices WHERE id = $1 AND company_id = $2`,
      [id, company_id]
    );

    if (parseInt(validation.rows[0].device_exists) === 0) {
      return res.status(404).json({ message: 'Device not found or unauthorized' });
    }

    await client.query('BEGIN');

    await client.query(`DELETE FROM senso.senso_device_calculation_config WHERE device_id = $1`, [id]);
    await client.query(`DELETE FROM senso.senso_device_users WHERE device_id = $1`, [id]);
    await client.query(`DELETE FROM senso.senso_devices WHERE id = $1 AND company_id = $2`, [id, company_id]);

    await client.query('COMMIT');

    res.status(200).json({ message: 'Device deleted successfully' });
  } catch (err) {
    await db.query('ROLLBACK');
    logger.error(`[DELETE DEVICE ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to delete device', error: 'Internal Server Error' });
  } finally {
    client.release();
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
    //console.time("TOTAL_API_TIME");
    const { company_id } = req.user;
    const { deviceuid, start_date, end_date } = req.query;

    if (!deviceuid || !start_date || !end_date) {
      return res.status(400).json({ message: 'deviceuid, start_date, and end_date are required' });
    }

    //console.time("DEVICE_CHECK");
    const deviceCheck = await db.query(
      `SELECT id FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2`,
      [deviceuid, company_id]
    );
    //console.timeEnd("DEVICE_CHECK");

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

    //console.time("DB_QUERY");
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
    //console.timeEnd("DB_QUERY");

    res.status(200).json({
      message: 'Data fetched successfully',
      bucket_interval: `${intervalMinutes} minutes`,
      count: result.rowCount,
      data: result.rows
    });

    // console.timeEnd("TOTAL_API_TIME");
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
        + floor((EXTRACT(minute FROM "timestamp") / $1)) * ($1 * interval '1 minute')
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


exports.getDeviceConsumption = async (req, res) => {
  try {
    const { company_id } = req.user;
    const { deviceuid, start_date, end_date } = req.query;

    if (!deviceuid || !start_date || !end_date) {
      return res.status(400).json({ message: 'deviceuid, start_date, and end_date are required' });
    }

    const deviceCheck = await db.query(
      `SELECT 1 FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2 LIMIT 1`,
      [deviceuid, company_id]
    );
    if (!deviceCheck.rowCount) {
      return res.status(403).json({ message: 'Unauthorized device access' });
    }

    const start = new Date(start_date);
    const end = new Date(end_date);
    const diffHours = (end - start) / 3600000;
    const diffDays = diffHours / 24;

    let bucketInterval, bucketLabel, truncateLevel;
    if (diffHours <= 1) {
      bucketInterval = '10 minutes';
      bucketLabel = '10 minutes';
      truncateLevel = 'minute';
    } else if (diffHours <= 24) {
      bucketInterval = '1 hour';
      bucketLabel = '1 hour';
      truncateLevel = 'hour';
    } else if (diffDays <= 45) {
      bucketInterval = '1 day';
      bucketLabel = '1 day';
      truncateLevel = 'day';
    } else {
      bucketInterval = '1 month';
      bucketLabel = '1 month';
      truncateLevel = 'month';
    }

    // ✅ Safe hardcoded whitelist check to prevent SQL injection
    const allowedTruncLevels = ['minute', 'hour', 'day', 'month'];
    if (!allowedTruncLevels.includes(truncateLevel)) {
      return res.status(400).json({ message: 'Invalid truncate level' });
    }

    const result = await db.query(
      `
      SELECT 
        $1 AS deviceuid,
        TO_CHAR(
          date_trunc('${truncateLevel}', "timestamp"),
          'YYYY-MM-DD"T"HH24:MI:SS"Z"'
        ) AS bucket_time,
        COALESCE(MAX(totalvolume) - MIN(totalvolume), 0) AS consumption
      FROM senso.senso_data
      WHERE deviceuid = $1
        AND "timestamp" BETWEEN $2 AND $3
        AND totalvolume IS NOT NULL
      GROUP BY date_trunc('${truncateLevel}', "timestamp")
      ORDER BY bucket_time ASC
      `,
      [deviceuid, start_date, end_date]
    );

    res.status(200).json({
      message: 'Consumption data fetched successfully',
      bucket_interval: bucketLabel,
      count: result.rowCount,
      data: result.rows
    });
  } catch (err) {
    logger.error(`[DEVICE CONSUMPTION ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch consumption data', error: err.message });
  }
};

exports.getDeviceSummary = async (req, res) => {
  try {
    const { company_id } = req.user;
    const { deviceuid, start_date, end_date } = req.query;

    if (!deviceuid || !start_date || !end_date) {
      return res.status(400).json({ message: 'deviceuid, start_date, and end_date are required' });
    }

    const deviceCheck = await db.query(
      `SELECT 1 FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2 LIMIT 1`,
      [deviceuid, company_id]
    );
    if (!deviceCheck.rowCount) {
      return res.status(403).json({ message: 'Unauthorized device access' });
    }

    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0).toISOString();
    const weekStart = new Date(now);
    weekStart.setDate(now.getDate() - now.getDay()); // Sunday as week start; change if Monday needed
    weekStart.setHours(0, 0, 0, 0);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0).toISOString();

    const queries = {
      flowStats: db.query(
        `
        SELECT 
          (SELECT flowrate FROM senso.senso_data WHERE deviceuid=$1 ORDER BY timestamp DESC LIMIT 1) AS current_flowrate,
          (SELECT pressure FROM senso.senso_data WHERE deviceuid=$1 ORDER BY timestamp DESC LIMIT 1) AS current_pressure,
          MAX(flowrate) AS max_flowrate,
          MIN(flowrate) AS min_flowrate,
          MAX(pressure) AS max_pressure,
          MIN(pressure) AS min_pressure,
          ROUND(AVG(flowrate)::numeric,2) AS avg_flowrate
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3
        `,
        [deviceuid, start_date, end_date]
      ),
      todayConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS today_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND NOW() AND totalvolume IS NOT NULL
        `,
        [deviceuid, todayStart]
      ),
      weekConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS week_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND NOW() AND totalvolume IS NOT NULL
        `,
        [deviceuid, weekStart.toISOString()]
      ),
      monthConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS month_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND NOW() AND totalvolume IS NOT NULL
        `,
        [deviceuid, monthStart]
      ),
      overallConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS overall_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, start_date, end_date]
      )
    };

    const [flowStats, todayCons, weekCons, monthCons, overallCons] = await Promise.all([
      queries.flowStats, queries.todayConsumption, queries.weekConsumption,
      queries.monthConsumption, queries.overallConsumption
    ]);

    res.status(200).json({
      message: 'Device summary fetched successfully',
      deviceuid,
      data: {
        CurrentFlowRate: parseFloat(flowStats.rows[0]?.current_flowrate || 0).toFixed(2),
        CurrentPressure: parseFloat(flowStats.rows[0]?.current_pressure || 0).toFixed(2),
        MaxPressure: parseFloat(flowStats.rows[0]?.max_pressure || 0).toFixed(2),
        MinPressure: parseFloat(flowStats.rows[0]?.min_pressure || 0).toFixed(2),
        MaxFlowRate: parseFloat(flowStats.rows[0]?.max_flowrate || 0).toFixed(2),
        MinFlowRate: parseFloat(flowStats.rows[0]?.min_flowrate || 0).toFixed(2),
        AvgFlowRate: parseFloat(flowStats.rows[0]?.avg_flowrate || 0).toFixed(2),
        TodayConsumption: parseFloat(todayCons.rows[0]?.today_consumption || 0).toFixed(2),
        WeekConsumption: parseFloat(weekCons.rows[0]?.week_consumption || 0).toFixed(2),
        MonthConsumption: parseFloat(monthCons.rows[0]?.month_consumption || 0).toFixed(2),
        OverallConsumption: parseFloat(overallCons.rows[0]?.overall_consumption || 0).toFixed(2)
      }
    });
  } catch (err) {
    logger.error(`[DEVICE SUMMARY ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch device summary', error: err.message });
  }
};

exports.getDeviceCalculationConfigs = async (req, res) => {
  try {
    const { company_id } = req.user;
    const { deviceuid } = req.query;

    if (!deviceuid) {
      return res.status(400).json({ message: 'deviceuid is required' });
    }

    // 1. Fetch calculation configs + notification configs in a single query
    const result = await db.query(
      `
      SELECT 
        c.id AS calc_id,
        c.device_id AS calc_device_id,
        c.parameter,
        c.custom_unit,
        c.tank_type,
        c.length,
        c.width,
        c.height,
        c.calc_operator,
        c.calc_operand,
        c.calc_expression,
        c.is_active AS calc_is_active,
        c.created_at AS calc_created_at,
        c.updated_at AS calc_updated_at,

        n.id AS notif_id,
        n.enabled AS notif_enabled,
        n.threshold_type,
        n."condition",
        n.threshold_value,
        n.notify_email,
        n.notify_whatsapp,
        n.notify_sms

      FROM senso.senso_devices d
      INNER JOIN senso.senso_device_calculation_config c 
        ON d.id = c.device_id
      LEFT JOIN senso.senso_device_notifications n 
        ON d.id = n.device_id

      WHERE d.device_uid = $1 AND d.company_id = $2
      ORDER BY c.id ASC, n.id ASC
      `,
      [deviceuid, company_id]
    );

    if (!result.rowCount) {
      return res.status(403).json({ message: 'Unauthorized device access or no configs found' });
    }

    // 2. Group data: calculationConfigs (unique) + notificationConfigs (grouped by device)
    const calcMap = new Map();
    const notifArr = [];

    result.rows.forEach(row => {
      // Map calculation configs uniquely by calc_id
      if (!calcMap.has(row.calc_id)) {
        calcMap.set(row.calc_id, {
          Id: row.calc_id,
          DeviceID: row.calc_device_id,
          Parameter: row.parameter,
          CustomUnit: row.custom_unit,
          TankType: row.tank_type,
          Length: parseFloat(row.length || 0),
          Width: parseFloat(row.width || 0),
          Height: parseFloat(row.height || 0),
          CalcOperator: row.calc_operator,
          CalcOperand: parseFloat(row.calc_operand || 0),
          CalcExpression: row.calc_expression,
          IsActive: row.calc_is_active,
          CreatedAt: row.calc_created_at,
          UpdatedAt: row.calc_updated_at
        });
      }

      // Push notification configs (if exists)
      if (row.notif_id) {
        notifArr.push({
          Id: row.notif_id,
          DeviceID: row.calc_device_id,
          Enabled: row.notif_enabled,
          ThresholdType: row.threshold_type,
          Condition: row.condition,
          ThresholdValue: parseFloat(row.threshold_value || 0),
          NotifyEmail: row.notify_email,
          NotifyWhatsapp: row.notify_whatsapp,
          NotifySms: row.notify_sms
        });
      }
    });

    res.status(200).json({
      message: 'Device calculation & notification configurations fetched successfully',
      deviceuid,
      deviceid: result.rows[0].calc_device_id,
      calculationConfigsCount: calcMap.size,
      notificationConfigsCount: notifArr.length,
      calculationConfigs: Array.from(calcMap.values()),
      notificationConfigs: notifArr
    });
  } catch (err) {
    logger.error(`[DEVICE CALC CONFIG ERROR] ${err.message}`);
    res.status(500).json({ message: 'Failed to fetch device calculation & notification configurations', error: err.message });
  }
};

exports.getDeviceConsumptionSummary = async (req, res) => {
  try {
    const { company_id } = req.user;
    const { deviceuid } = req.query;

    if (!deviceuid) {
      return res.status(400).json({ message: 'deviceuid is required' });
    }

    // Check device access
    const deviceCheck = await db.query(
      `SELECT 1 FROM senso.senso_devices WHERE device_uid = $1 AND company_id = $2 LIMIT 1`,
      [deviceuid, company_id]
    );
    if (!deviceCheck.rowCount) {
      return res.status(403).json({ message: 'Unauthorized device access' });
    }

    // Date Calculations
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);

    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0);
    const yearStart = new Date(now.getFullYear(), 0, 1, 0, 0, 0);

    const yesterdayStart = new Date(todayStart);
    yesterdayStart.setDate(todayStart.getDate() - 1);
    const yesterdayEnd = new Date(todayStart);
    yesterdayEnd.setMilliseconds(-1); // yesterday 23:59:59.999

    const lastMonthStart = new Date(monthStart);
    lastMonthStart.setMonth(monthStart.getMonth() - 1);
    const lastMonthEnd = new Date(monthStart);
    lastMonthEnd.setMilliseconds(-1); // last month 23:59:59

    const lastYearStart = new Date(yearStart);
    lastYearStart.setFullYear(yearStart.getFullYear() - 1);
    const lastYearEnd = new Date(yearStart);
    lastYearEnd.setMilliseconds(-1); // last year 23:59:59

    // Queries
    const queries = {
      todayConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS today_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, todayStart.toISOString(), now.toISOString()]
      ),
      monthConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS month_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, monthStart.toISOString(), now.toISOString()]
      ),
      yearConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS year_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, yearStart.toISOString(), now.toISOString()]
      ),
      yesterdayConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS yesterday_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, yesterdayStart.toISOString(), yesterdayEnd.toISOString()]
      ),
      lastMonthConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS last_month_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, lastMonthStart.toISOString(), lastMonthEnd.toISOString()]
      ),
      lastYearConsumption: db.query(
        `
        SELECT COALESCE(MAX(totalvolume) - MIN(totalvolume),0) AS last_year_consumption
        FROM senso.senso_data
        WHERE deviceuid=$1 AND timestamp BETWEEN $2 AND $3 AND totalvolume IS NOT NULL
        `,
        [deviceuid, lastYearStart.toISOString(), lastYearEnd.toISOString()]
      )
    };

    const [
      todayCons,
      monthCons,
      yearCons,
      yesterdayCons,
      lastMonthCons,
      lastYearCons
    ] = await Promise.all([
      queries.todayConsumption,
      queries.monthConsumption,
      queries.yearConsumption,
      queries.yesterdayConsumption,
      queries.lastMonthConsumption,
      queries.lastYearConsumption
    ]);

    res.status(200).json({
      message: 'Device consumption summary fetched successfully',
      deviceuid,
      data: {
        Today: parseFloat(todayCons.rows[0]?.today_consumption || 0).toFixed(2),
        ThisMonth: parseFloat(monthCons.rows[0]?.month_consumption || 0).toFixed(2),
        ThisYear: parseFloat(yearCons.rows[0]?.year_consumption || 0).toFixed(2),
        Yesterday: parseFloat(yesterdayCons.rows[0]?.yesterday_consumption || 0).toFixed(2),
        LastMonth: parseFloat(lastMonthCons.rows[0]?.last_month_consumption || 0).toFixed(2),
        LastYear: parseFloat(lastYearCons.rows[0]?.last_year_consumption || 0).toFixed(2)
      }
    });
  } catch (err) {
    logger.error(`[DEVICE CONSUMPTION SUMMARY ERROR] ${err.message}`);
    res.status(500).json({
      message: 'Failed to fetch device consumption summary',
      error: err.message
    });
  }
};

