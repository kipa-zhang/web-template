const express = require('express');
const router = express.Router();
require('express-async-errors');

const mtAdminService = require('../services/mtAdminService');

router.post('/getPurposeModelist', mtAdminService.getPurposeModelist);
router.get('/getPurposeModeType', mtAdminService.getPurposeModeType);
router.post('/getHubNode', mtAdminService.getHubNode);
router.post('/getUnitIdByUserId', mtAdminService.getUnitIdByUserId);
router.post('/GetDestination', mtAdminService.GetDestination);
router.post('/getVehicleType', mtAdminService.getVehicleType);
router.post('/getDriverList', mtAdminService.getDriverList);
router.post('/getVehicleList', mtAdminService.getVehicleList);
router.post('/getVehicleListByTaskId', mtAdminService.getVehicleListByTaskId);
router.post('/getMtAdminList', mtAdminService.getMtAdminList);
router.post('/getMtAdminByMtAdminId', mtAdminService.getMtAdminByMtAdminId);
router.post('/createMtAdmin', mtAdminService.createMtAdmin);
router.post('/deleteMtAdminByMtAdminId', mtAdminService.deleteMtAdminByMtAdminId);
router.post('/updateMtAdminByMtAdminId', mtAdminService.updateMtAdminByMtAdminId);
router.post('/getUnitId', mtAdminService.getUnitId);
router.post('/verifyVehicleType', mtAdminService.verifyVehicleType);

router.post('/getVehicleTypeByGroup', mtAdminService.getVehicleTypeByGroup);
router.post('/getVehicleNoByGroup', mtAdminService.getVehicleNoByGroup);
router.post('/getDriverDatatByGroup', mtAdminService.getDriverDatatByGroup);
router.post('/getUserByUserId', mtAdminService.getUserByUserId);

module.exports = router;
