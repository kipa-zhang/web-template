const express = require('express');
const router = express.Router();
require('express-async-errors');

const hqDashboardService = require('../services/hqDashboardService');

router.post('/getAllUnits', hqDashboardService.GetAllUnits);
router.post('/getMilitaryVehicleAndIndents', hqDashboardService.GetMilitaryVehicleAndIndents);
router.post('/getWPTDueThisWeek', hqDashboardService.GetWPTDueThisWeek);
router.post('/getActivityTypeAndVehicleType', hqDashboardService.GetActivityTypeAndVehicleType);
router.post('/getIndentAllocationGraph', hqDashboardService.getIndentAllocationGraph);
router.post('/getVehicleAvailabilityGraph', hqDashboardService.GetVehicleAvailabilityGraph);
router.post('/getTOAvailabilityGraph', hqDashboardService.GetTOAvailabilityGraph);
router.post('/getVehicleServicingGraph', hqDashboardService.GetVehicleServicingGraph);

module.exports = router;
