const { SignJWT, jwtVerify } = require('jose');
const { getPrivateKey, getPublicKey } = require('../config/keys'); // RSA key loader
require('dotenv').config();

const defaultOptions = {
  expiresIn: '1h',
  algorithm: 'RS256',
};

// Generate a JWT token
async function generateToken(payload, options = {}) {
  const privateKey = await getPrivateKey();
  const mergedOptions = { ...defaultOptions, ...options };

  const jwt = await new SignJWT(payload)
    .setProtectedHeader({ alg: mergedOptions.algorithm })
    .setIssuedAt()
    .setExpirationTime(mergedOptions.expiresIn)
    .sign(privateKey);

  return jwt;
}

// Verify a JWT token
async function verifyToken(token) {
  try {
    const publicKey = await getPublicKey();
    const { payload } = await jwtVerify(token, publicKey);
    return payload;
  } catch (error) {
    if (error.code === 'ERR_JWT_EXPIRED') {
      throw new Error('Token has expired');
    } else if (error.code === 'ERR_JWS_SIGNATURE_VERIFICATION_FAILED') {
      throw new Error('Invalid token signature');
    } else {
      throw new Error('Token verification failed');
    }
  }
}

// Refresh token (ignore expiration)
async function refreshToken(token, options = {}) {
  try {
    const publicKey = await getPublicKey();
    const { payload } = await jwtVerify(token, publicKey, { ignoreExpiration: true });

    delete payload.iat;
    delete payload.exp;
    delete payload.nbf;

    return await generateToken(payload, options);
  } catch (error) {
    throw new Error('Token refresh failed');
  }
}

// Express middleware to authenticate user
async function authenticateUser(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) {
    return res.status(401).json({ message: 'Authorization token is required' });
  }
  try {
    const decoded = await verifyToken(token);
    req.user = decoded;
    next();
  } catch (error) {
    res.status(401).json({ message: error.message });
  }
}

module.exports = {
  generateToken,
  verifyToken,
  refreshToken,
  authenticateUser,
};
