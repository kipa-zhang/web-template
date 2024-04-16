const express = require('express');
const router = express.Router();
require('express-async-errors');

const userService = require('../services/userService');
const vehicleService = require('../services/vehicleService');
const vehicleKeyService = require('../services/vehicleKeyService.js');

router.get('/vehicleTask', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Resources', 'Vehicle List')
    res.render('vehicle/saf-vehicles', { title: 'Vehicle Task', pageList });
});

router.get('/vehicleDetail', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Vehicle')
    res.render('vehicle/vehicle-info', { title: 'Vehicle Detail', pageList });
});

router.get('/schedule', (req, res) => {
    res.render('vehicle/vehicle-schedule', { title: 'Vehicle Schedule' });
});

router.post('/editVehicle', vehicleService.editVehicle);
router.post('/getVehicleList', vehicleService.getVehicleList);
router.post('/getVehicleTasks', vehicleService.getVehicleTasks);
router.post('/getVehicleAssignedTasks', vehicleService.getVehicleAssignedTasks);
router.post('/getVehicleLastPosition', vehicleService.getVehicleLastPosition);
router.post('/getVehicleDetail', vehicleService.getVehicleDetail);
router.post('/getVehicleOdd', vehicleService.getVehicleOdd);

router.post('/getTOVehicleStatusList', vehicleService.getTOVehicleStatusList);

router.post('/getPurpose', vehicleService.getPurpose);
router.post('/getVehicleSchedule', vehicleService.getVehicleSchedule);
router.post('/getVehicleMaintenance', vehicleService.getVehicleMaintenance);

router.post('/getTypeOfVehicle', vehicleService.getTypeOfVehicle);
router.post('/getPermitTypeByVehicleType', vehicleService.getPermitTypeByVehicleType);
router.post('/getCategoryOfVehicle', vehicleService.getCategoryOfVehicle);
router.post('/deleteVehicle', vehicleService.deleteVehicle);
router.post('/updateVehicleStatus', vehicleService.updateVehicleStatus);
router.post('/getParadeStateSubmitTime', vehicleService.getParadeStateSubmitTime);
router.post('/oddRectify', vehicleService.oddRectify);

router.post('/markAsUnavailable', vehicleService.markAsUnavailable);
router.post('/cancelMarkAsUnavailable', vehicleService.cancelMarkAsUnavailable);
router.post('/getLeaveRecordByDate', vehicleService.getLeaveRecordByDate);
router.post('/getVehicleLeaveDays', vehicleService.getVehicleLeaveDays);
router.post('/getVehicleListByGroup', vehicleService.getVehicleListByGroup);

// router.post('/releaseVehicle', vehicleService.releaseVehicle);

router.post('/reactivateVehicle', vehicleService.reactivateVehicle);

router.post('/parseKeyBoxQRCode', vehicleKeyService.parseKeyBoxQRCode);

router.post('/getSupportSiteList', vehicleKeyService.getSupportSiteList);

router.post('/createKeyOptRecord', vehicleKeyService.createKeyOptRecord);

router.post('/getKeyBoxPageList', vehicleKeyService.getKeyBoxPageList);

router.post('/getKeyBoxDetailPageList', vehicleKeyService.getKeyBoxDetailPageList);

router.post('/getKeyTransactionsPageList', vehicleKeyService.getKeyTransactionsPageList);

router.post('/generateKeypressTransactionQRCode', vehicleKeyService.generateKeypressTransactionQRCode);

router.post('/uploadKeyOptRecord', vehicleKeyService.uploadKeyOptRecord);

router.post('/getKeyBoxSummaryList', vehicleKeyService.getKeyBoxSummaryList);
router.post('/getUnitKeySummary', vehicleKeyService.getUnitKeySummary);

router.post('/getUserVehicleSummaryList', vehicleService.getUserVehicleSummaryList);

router.post('/updateVehicleAviDate', vehicleService.updateVehicleAviDate);

router.post('/getVehicleEffectiveData', vehicleService.getVehicleEffectiveData);

router.post('/getVehicleTypeList', vehicleService.getVehicleTypeList);
router.post('/updateVehicleType', vehicleService.updateVehicleType);
router.post('/activateVehicleType', vehicleService.activateVehicleType);

module.exports = router;
