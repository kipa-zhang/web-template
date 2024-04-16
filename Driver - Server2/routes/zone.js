const express = require('express');
const router = express.Router();
require('express-async-errors');

const userService = require('../services/userService');
const groupService = require('../services/groupService');
const zoneService = require('../services/zoneService');

router.post('/getUserZoneList', zoneService.getUserZoneList);
router.post('/getUserZoneUserList', zoneService.getUserZoneUserList);
router.post('/createUserZone', zoneService.createUserZone);
router.post('/deleteUserZone', zoneService.deleteUserZone);
router.post('/updateUserZone', zoneService.updateUserZone);

router.post('/getNogoZoneList', zoneService.getNogoZoneList);
router.post('/createNogoZone', zoneService.createNogoZone);
router.post('/deleteNogoZone', zoneService.deleteNogoZone);
router.post('/updateNogoZone', zoneService.updateNogoZone);
router.post('/checkNogoZoneName', zoneService.checkNogoZoneName);
router.post('/updateNogoZoneStatus', zoneService.updateNogoZoneStatus);

module.exports = router;
