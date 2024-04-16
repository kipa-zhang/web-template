const express = require('express');
const router = express.Router();
require('express-async-errors');

const unitService = require('../services/unitService');

router.post('/getAllHubNodeList', unitService.getAllHubNodeList);
router.post('/getHubNodeList', unitService.getHubNodeList);
router.post('/getUnitPermissionList', unitService.getUnitPermissionList);
router.post('/getSubUnitPermissionList2', unitService.getSubUnitPermissionList2);
router.post('/getSubUnitPermissionList', unitService.getSubUnitPermissionList);

router.post('/getPermitUnitList2', unitService.getPermitUnitList);

module.exports = router;
