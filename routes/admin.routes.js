const express = require('express');
const router = express.Router();
const adminController = require('../controllers/admin.controller');
const { authenticateUser } = require('../utils/auth.jose');

/* ---------- USERS ---------- */
router.get('/users', authenticateUser, adminController.getUsersFromTokenCompany);      // Get all users
router.post('/users', authenticateUser, adminController.addUser);                      // Add new user
router.put('/users', authenticateUser, adminController.updateUser);                    // Update user
router.delete('/users/:id', authenticateUser, adminController.deleteUser);             // Delete user

/* ---------- ZONES ---------- */
router.get('/zones', authenticateUser, adminController.getZonesFromTokenCompany);      // Get zones
router.post('/zones', authenticateUser, adminController.addZone);                      // Add zone
router.put('/zones/:id', authenticateUser, adminController.updateZone);                // Update zone
router.delete('/zones/:id', authenticateUser, adminController.deleteZone);             // Delete zone

/* ---------- DEVICE TYPES ---------- */
router.get('/device-types', authenticateUser, adminController.getDeviceTypes);         // Get types
router.post('/device-types', authenticateUser, adminController.addDeviceType);         // Add type
router.put('/device-types', authenticateUser, adminController.updateDeviceType);       // Update type
router.delete('/device-types/:id', authenticateUser, adminController.deleteDeviceType);// Delete type

/* ---------- DEVICES ---------- */
router.get('/devices', authenticateUser, adminController.getDevicesFromTokenCompany);  // Get devices
router.post('/devices', authenticateUser, adminController.addDevice);                  // Add device
router.put('/devices/:id', authenticateUser, adminController.updateDevice);            // Update device
router.delete('/devices/:id', authenticateUser, adminController.deleteDevice);         // Delete device

/* ---------- DEVICE WIDGETS ---------- */
router.get('/devices/:device_id/widgets', authenticateUser, adminController.getWidgetsByDevice);
router.post('/widgets', authenticateUser, adminController.addWidget);
router.put('/widgets', authenticateUser, adminController.updateWidget);
router.delete('/widgets/:id', authenticateUser, adminController.deleteWidget);

/* ---------- DEVICE DATA & ANALYTICS ---------- */
router.get('/device-data', authenticateUser, adminController.getDeviceDataWithBucketing);
router.get('/latest', authenticateUser, adminController.getLatestTwoEntriesPerDevice);
router.get('/device-data/bucket', authenticateUser, adminController.getDeviceDataWithBucket);
router.get('/device/consumption', authenticateUser, adminController.getDeviceConsumption);
router.get('/device/summary', authenticateUser, adminController.getDeviceSummary);
router.get('/device/calculation-configs', authenticateUser, adminController.getDeviceCalculationConfigs);
router.get('/device/summaryConsumption', authenticateUser, adminController.getDeviceConsumptionSummary);

/* ---------- ALERTS ---------- */
router.get('/alerts', authenticateUser, adminController.getAlerts);                    // Get alerts
router.post('/alerts', authenticateUser, adminController.addAlert);                    // Add alert (duplicate-check)
router.put('/alerts/:id', authenticateUser, adminController.updateAlert);              // Update alert
router.delete('/alerts/:id', authenticateUser, adminController.deleteAlert);           // Delete alert

module.exports = router;
