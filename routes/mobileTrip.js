const express = require('express');
const router = express.Router();
require('express-async-errors');

const tripService = require('../service/tripService')

router.post('/getSystemLocation', tripService.getSystemLocation);
router.post('/getPurpose', tripService.getPurpose);
router.post('/getVehicle', tripService.getVehicle);
router.post('/addTrip', tripService.addTrip);

module.exports = router;