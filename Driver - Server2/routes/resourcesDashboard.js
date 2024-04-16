const express = require('express');
const router = express.Router();
require('express-async-errors');

const resourcesDashboardService = require('../services/resourcesDashboardService');

router.post('/getDriverAndVehicleDeployableTotalByHub', resourcesDashboardService.getDriverAndVehicleDeployableTotalByHub);
router.post('/getDriverAndVehicleDeployableTotalByNode', resourcesDashboardService.getDriverAndVehicleDeployableTotalByNode);
router.post('/getDriverAndVehicleDeployedTotalByNode', resourcesDashboardService.getDriverAndVehicleDeployedTotalByNode);
router.post('/getDriverAndVehicleDeployedTotalByHub', resourcesDashboardService.getDriverAndVehicleDeployedTotalByHub);
router.post('/getDriverAndVehicleAvailabilityByNode', resourcesDashboardService.getDriverAndVehicleAvailabilityByNode);
router.post('/getDriverAndVehicleAvailabilityByHub', resourcesDashboardService.getDriverAndVehicleAvailabilityByHub);
router.post('/getDriverByRoleByHub', resourcesDashboardService.getDriverByRoleByHub);
router.post('/getDriverByRoleByNode', resourcesDashboardService.getDriverByRoleByNode);
router.post('/getVehicleByPurposeByHub', resourcesDashboardService.getVehicleByPurposeByHub);
router.post('/getVehicleByPurposeByNode', resourcesDashboardService.getVehicleByPurposeByNode);
router.post('/getDriverTotalByRoleByHub', resourcesDashboardService.getDriverTotalByRoleByHub);
router.post('/getDriverTotalByRoleByNode', resourcesDashboardService.getDriverTotalByRoleByNode);
router.post('/getTaskTotalByPurposeByHub', resourcesDashboardService.getTaskTotalByPurposeByHub);
router.post('/getTaskTotalByPurposeByNode', resourcesDashboardService.getTaskTotalByPurposeByNode);

router.post('/getVehicleTotalByLoanOut', resourcesDashboardService.getVehicleTotalByLoanOut)
// router.post('/getVehicleTotalByOnhold', resourcesDashboardService.getVehicleTotalByOnhold)
router.post('/getDriverTotalByLoanOut', resourcesDashboardService.getDriverTotalByLoanOut)
router.post('/getDriverTotalByStatusInvalid', resourcesDashboardService.getDriverTotalByStatusInvalid)

module.exports = router;
