const db = require('../config/db');
const bcrypt = require('bcrypt');
const { generateToken, verifyToken } = require('../utils/auth.jose');
const logger = require('../utils/logger');

exports.login = async (req, res) => {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ message: 'Email and password are required' });
  }

  try {
    const result = await db.query(
      `SELECT id, email, password, user_type, first_name, last_name, company_id, zone_id, blocked, verified FROM senso.senso_users WHERE email = $1`,
      [email]
    );

    if (result.rows.length === 0) {
      logger.warn(`[LOGIN FAIL] Invalid email: ${email}`);
      return res.status(401).json({ message: 'Invalid email or password' });
    }

    const user = result.rows[0];

    if (user.blocked) {
      logger.warn(`[LOGIN BLOCKED] Blocked user attempted login: ${email}`);
      return res.status(403).json({ message: 'Account is blocked' });
    }

    if (!user.verified) {
      logger.warn(`[LOGIN UNVERIFIED] Unverified user attempted login: ${email}`);
      return res.status(403).json({ message: 'Account not verified. Please verify your email.' });
    }

    const match = await bcrypt.compare(password, user.password);
    if (!match) {
      logger.warn(`[LOGIN FAIL] Wrong password: ${email}`);
      return res.status(401).json({ message: 'Invalid email or password' });
    }

    await db.query(
      `UPDATE senso.senso_users SET is_online = true WHERE id = $1`,
      [user.id]
    );

    const token = await generateToken({
      id: user.id,
      email: user.email,
      role: user.user_type,
      company_id: user.company_id,
      zone_id: user.zone_id
    });

    logger.info(`[LOGIN SUCCESS] ${email} from IP ${req.ip} using ${req.headers['user-agent']}`);
    res.status(200).json({
      message: 'Login successful',
      token,
      user: {
        id: user.id,
        email: user.email,
        role: user.user_type,
        name: `${user.first_name} ${user.last_name}`,
        company_id: user.company_id,
        zone_id: user.zone_id
      }
    });
  } catch (err) {
    console.error('[LOGIN ERROR]', err);
    logger.error(`[LOGIN ERROR] ${err.message}`);
    res.status(500).json({ message: 'Internal Server Error' });
  }
};

exports.register = async (req, res) => {
  const client = await db.pool.connect();

  const {
    company_name,
    location,
    contact_number,
    official_email,
    personal_email,
    first_name,
    last_name,
    designation,
    password
  } = req.body;

  if (!company_name || !location || !contact_number || !official_email || !personal_email || !first_name || !last_name || !designation || !password) {
    return res.status(400).json({ message: 'All fields are required' });
  }

  try {
    await client.query('BEGIN');

    const hashedPassword = await bcrypt.hash(password, 10);

    const companyRes = await client.query(
      `INSERT INTO senso.senso_companies (company_name, location, official_email, contact_number)
       VALUES ($1, $2, $3, $4)
       RETURNING id`,
      [company_name, location, official_email, contact_number]
    );

    const company_id = companyRes.rows[0].id;

    const zoneRes = await client.query(
      `INSERT INTO senso.senso_zones (zone_name, company_id)
       VALUES ($1, $2)
       RETURNING id`,
      ['Main Zone', company_id]
    );

    const zone_id = zoneRes.rows[0].id;

    await client.query(
      `INSERT INTO senso.senso_users 
       (username, first_name, last_name, email, password, contact_number, designation, user_type, verified, blocked, company_id, zone_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'Admin', false, false, $8, $9)`,
      [personal_email, first_name, last_name, personal_email, hashedPassword, contact_number, designation, company_id, zone_id]
    );

    const verificationToken = await generateToken({ email: personal_email }, { expiresIn: '12h' });

    await client.query('COMMIT');
    res.status(201).json({ message: 'Registration successful. Please verify your email.', token: verificationToken });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[REGISTRATION ERROR]', err);
    res.status(500).json({ message: 'Registration failed', error: err.message });
  } finally {
    client.release();
  }
};

exports.verifyEmail = async (req, res) => {
  const { token } = req.query;
  try {
    const payload = await verifyToken(token);
    const email = payload.email;

    const result = await db.query(
      `UPDATE senso.senso_users SET verified = true WHERE email = $1 RETURNING id`,
      [email]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found or already verified' });
    }

    res.status(200).json({ message: 'Email verified successfully' });
  } catch (err) {
    console.error('[EMAIL VERIFICATION ERROR]', err);
    res.status(400).json({ message: 'Invalid or expired verification token' });
  }
};

exports.resendVerification = async (req, res) => {
  const { email } = req.body;

  if (!email) {
    return res.status(400).json({ message: 'Email is required' });
  }

  try {
    const result = await db.query(
      `SELECT verified FROM senso.senso_users WHERE email = $1`,
      [email]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found' });
    }

    const { verified } = result.rows[0];

    if (verified) {
      return res.status(200).json({ message: 'User is already verified' });
    }

    const verificationToken = await generateToken({ email }, { expiresIn: '12h' });

    res.status(200).json({ message: 'Verification token generated', token: verificationToken });
  } catch (err) {
    console.error('[RESEND VERIFICATION ERROR]', err);
    res.status(500).json({ message: 'Failed to generate verification token', error: err.message });
  }
};

exports.forgotPassword = async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ message: 'Email is required' });

  try {
    const result = await db.query(
      `SELECT id FROM senso.senso_users WHERE email = $1`,
      [email]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found' });
    }

    const token = await generateToken({ email }, { expiresIn: '1h' });

    res.status(200).json({
      message: 'Password reset token generated',
      token
    });
  } catch (err) {
    console.error('[FORGOT PASSWORD ERROR]', err);
    res.status(500).json({ message: 'Failed to generate token', error: err.message });
  }
};

exports.resetPassword = async (req, res) => {
  const { token, newPassword } = req.body;

  if (!token || !newPassword) {
    return res.status(400).json({ message: 'Token and new password are required' });
  }

  try {
    const payload = await verifyToken(token);
    const email = payload.email;

    const hashedPassword = await bcrypt.hash(newPassword, 10);

    const result = await db.query(
      `UPDATE senso.senso_users SET password = $1 WHERE email = $2 RETURNING id`,
      [hashedPassword, email]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'User not found' });
    }

    res.status(200).json({ message: 'Password updated successfully' });
  } catch (err) {
    console.error('[RESET PASSWORD ERROR]', err);
    res.status(400).json({ message: 'Invalid or expired token', error: err.message });
  }
};


exports.refreshToken = async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ message: 'Refresh token is required' });

  try {
    const newToken = await refreshToken(token, { expiresIn: '1h' });
    res.status(200).json({ token: newToken });
  } catch (err) {
    console.error('[REFRESH ERROR]', err);
    res.status(400).json({ message: 'Token refresh failed', error: err.message });
  }
};


exports.logout = async (req, res) => {
  try {
    const userId = req.user?.id;

    if (!userId) {
      return res.status(400).json({ message: 'User ID not found in token' });
    }

    await db.query(
      `UPDATE senso.senso_users SET is_online = false WHERE id = $1`,
      [userId]
    );

    res.status(200).json({ message: 'Logged out and status updated' });
  } catch (err) {
    console.error('[LOGOUT ERROR]', err);
    res.status(500).json({ message: 'Logout failed', error: err.message });
  }
};

