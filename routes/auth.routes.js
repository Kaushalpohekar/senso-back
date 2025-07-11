const express = require('express');
const router = express.Router();
const authController = require('../controllers/auth.controller');
const { authenticateUser } = require('../utils/auth.jose');

router.post('/register', authController.register);
router.post('/login', authController.login);

router.get('/verify', authenticateUser, (req, res) => {
  res.json({ message: 'Verified', user: req.user });
});

router.get('/verify-email', authController.verifyEmail);
router.post('/resend-verification', authController.resendVerification);

router.post('/forgot-password', authController.forgotPassword);
router.post('/reset-password', authController.resetPassword);

router.post('/refresh', authController.refreshToken);
router.post('/logout', authenticateUser, authController.logout);

module.exports = router;
