const express = require('express');
const router = express.Router();
require('express-async-errors');

const utils = require('../util/utils');

const userService = require('../services/userService');
const trackService = require('../services/trackService');

router.get('/', async (req, res) => {

    let pageList = await userService.getUserPageList(req.cookies.userId)

    utils.generateMapCookie(pageList, res)

    res.render('track/track', { title: 'User Track' });
});

router.post('/getTrackDashboardInfo', trackService.getTrackDashboardInfo);
router.post('/getEventHistory', trackService.getEventHistory);
router.post('/getAllEventHistory', trackService.getAllEventHistory);
router.post('/getEventLatestSpeedInfo', trackService.getEventLatestSpeedInfo);

// use for event page(not used now)
// router.post('/getDriverAndDeviceList', trackService.getDriverAndDeviceList);

// use for track page
router.post('/getDriverAndDeviceList', trackService.getDriverAndDeviceList2);

router.post('/getDriverAndDevicePositionList', trackService.getDriverAndDevicePositionList);

router.post('/getEventPositionHistory', trackService.getEventPositionHistory);

router.post('/getDriverLastPosition', trackService.getDriverLastPosition);
router.post('/getDriverLastPositionByVehicleNo', trackService.getDriverLastPositionByVehicleNo);

module.exports = router;
