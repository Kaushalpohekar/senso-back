const express = require('express');
const router = express.Router();
const adminController = require('../controllers/admin.controller');
const { authenticateUser } = require('../utils/auth.jose');

/* ------------------- USERS ------------------- */
router.get('/users', authenticateUser, adminController.getUsersFromTokenCompany);
router.post('/users', authenticateUser, adminController.addUser);
router.put('/users', authenticateUser, adminController.updateUser);
router.delete('/users/:id', authenticateUser, adminController.deleteUser);

/* ------------------- ZONES ------------------- */
router.get('/zones', authenticateUser, adminController.getZonesFromTokenCompany);
router.post('/zones', authenticateUser, adminController.addZone);
router.put('/zones', authenticateUser, adminController.updateZone);
router.delete('/zones/:id', authenticateUser, adminController.deleteZone);

/* ------------------- DEVICE TYPES ------------------- */
router.get('/device-types', authenticateUser, adminController.getDeviceTypes);
router.post('/device-types', authenticateUser, adminController.addDeviceType);
router.put('/device-types', authenticateUser, adminController.updateDeviceType);
router.delete('/device-types/:id', authenticateUser, adminController.deleteDeviceType);

/* ------------------- DEVICES ------------------- */
router.get('/devices', authenticateUser, adminController.getDevicesFromTokenCompany);
router.post('/devices', authenticateUser, adminController.addDevice);
router.put('/devices/:id', authenticateUser, adminController.updateDevice);
router.delete('/devices/:id', authenticateUser, adminController.deleteDevice);

/* ------------------- DEVICE WIDGETS ------------------- */
router.get('/devices/:device_id/widgets', authenticateUser, adminController.getWidgetsByDevice);
router.post('/widgets', authenticateUser, adminController.addWidget);
router.put('/widgets', authenticateUser, adminController.updateWidget);
router.delete('/widgets/:id', authenticateUser, adminController.deleteWidget);

router.get('/device-data', authenticateUser, adminController.getDeviceDataWithBucketing);
router.get('/latest', authenticateUser, adminController.getLatestTwoEntriesPerDevice);
router.get('/device-data/bucket', authenticateUser, adminController.getDeviceDataWithBucket);
router.get('/device/consumption', authenticateUser, adminController.getDeviceConsumption);
router.get('/device/summary', authenticateUser, adminController.getDeviceSummary);
router.get('/device/calculation-configs', authenticateUser, adminController.getDeviceCalculationConfigs);
router.get('/device/summaryConsumption', authenticateUser, adminController.getDeviceConsumptionSummary);
module.exports = router;
