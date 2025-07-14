const db = require('../config/db');
const logger = require('../utils/logger');

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
