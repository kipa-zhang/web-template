const express = require('express');
const router = express.Router();
require('express-async-errors');

const utils = require('../util/utils');
const indentAssignedService = require('../services/indentAssignedService');

router.get('/', (req, res) => {
    res.render('indentAssigned/index', { title: 'Trip Assign', userType: req.cookies.userType });
});

router.post('/getVehicleType', indentAssignedService.getVehicleType);
router.post('/getAssignableTaskList', indentAssignedService.getAssignableTaskList);

module.exports = router;
