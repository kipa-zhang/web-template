const express = require('express');
const router = express.Router();
require('express-async-errors');

const trafficService = require('../services/trafficService');

router.post('/getEstTravelTimes', trafficService.getEstTravelTimes);
router.post('/getTrafficList', trafficService.getTrafficList);
router.post('/getTrafficImages', trafficService.getTrafficImages);
router.post('/getTrafficSpeedBands', trafficService.getTrafficSpeedBands);

module.exports = router;