const express = require('express');
const router = express.Router();
const adminController = require('../controllers/admin.controller');
const { authenticateUser } = require('../utils/auth.jose');

router.get('/users', authenticateUser, adminController.getUsersFromTokenCompany);
router.get('/zones', authenticateUser, adminController.getZonesFromTokenCompany);

router.post('/users', authenticateUser, adminController.addUser);
router.put('/users', authenticateUser, adminController.updateUser);
router.delete('/users/:id', authenticateUser, adminController.deleteUser);


module.exports = router;
