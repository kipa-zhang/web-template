const express = require('express');
const router = express.Router();
require('express-async-errors');

const userService = require('../services/userService');
const driverService = require('../services/driverService');

router.post('/createDriverTask', driverService.createDriverTask);
router.post('/getDriverTaskList', driverService.getDriverTaskList);
router.post('/getDriverTaskById', driverService.getDriverTaskById);
router.post('/deleteDriverTask', driverService.deleteDriverTask);
router.post('/updateDriverTask', driverService.updateDriverTask);

router.post('/getDriverList', driverService.getDriverList);

router.post('/deleteDriver', driverService.deleteDriver);
// Not used yet
router.post('/createDriver', driverService.createDriver);


router.post('/updateDriverStatus', driverService.updateDriverStatus);

// TO Version
router.get('/driverTask', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Resources', 'TO List')
    res.render('driverTO/driver-view.html', { pageList });
});
router.get('/driver-info', async (req, res) => {
    let pageList1 = await userService.getUserPageList(req.cookies.userId, 'TO')
    let pageList2 = await userService.getUserPageList(req.cookies.userId, 'License')
    let pageList = pageList1.concat(pageList2)
    res.render('driverTO/driver-info.html', { pageList });
});
router.get('/calender', (req, res) => {
    res.render('driverTO/calender-view.html');
});

router.post('/getPermitTypeList', driverService.getPermitTypeList);
router.post('/getTODriverStatusList', driverService.getTODriverStatusList);

router.post('/getTODriverList', driverService.getTODriverList);
router.post('/getTODriverAssignedTasks', driverService.getTODriverAssignedTasks);
router.post('/getTOCalenderDriverList', driverService.getTOCalenderDriverList);
router.post('/getTODriverDetailInfo', driverService.getTODriverDetailInfo);
router.post('/getDriverMileageStatInfo', driverService.getDriverMileageStatInfo);
router.post('/getDriverTaskList2', driverService.getDriverTaskList2);
router.post('/getDriverTaskByTaskId', driverService.getDriverTaskByTaskId);
router.post('/reassignDriverTask', driverService.reassignDriverTask);
router.post('/markAsUnavailable', driverService.markAsUnavailable);
router.post('/getDriverLeaveDays', driverService.getDriverLeaveDays);
router.post('/cancelMarkAsUnavailable', driverService.cancelMarkAsUnavailable);
router.post('/getLeaveRecordByDate', driverService.getLeaveRecordByDate);
router.post('/getDriverAndVehicleForTask', driverService.getDriverAndVehicleForTask);

router.post('/getDriverByDriverId', driverService.getDriverByDriverId);
router.post('/getUnitList', driverService.getUnitList);

router.post('/getVehicleTypeByPermitType', driverService.getVehicleTypeByPermitType);

router.post('/getDriverIncidentList', driverService.getDriverIncidentList);
router.post('/createDriverIncident', driverService.createDriverIncident);
router.post('/getPlatformList', driverService.getPlatformList);
router.post('/getPlatformListGroupByVehicleType', driverService.getPlatformListGroupByVehicleType);

router.post('/getLicensingDriverList', driverService.getLicensingDriverList);

router.post('/updateAssessmentRecord', driverService.updateAssessmentRecord);
router.post('/getAssessmentRecord', driverService.getAssessmentRecord);
router.post('/deleteAssessmentRecord', driverService.deleteAssessmentRecord);

router.post('/getPlatformConfList', driverService.getPlatformConfList);
router.post('/updatePlatformConf', driverService.updatePlatformConf);
router.post('/deleteDriverPlatformConf', driverService.deleteDriverPlatformConf);
router.post('/getVehicleTypeByDriverId', driverService.getVehicleTypeByDriverId);

router.post('/getPermitTypeDetailList', driverService.getPermitTypeDetailList);
router.post('/updatePermitTypeDetail', driverService.updatePermitTypeDetail);
router.post('/deletePermitTypeDetail', driverService.deletePermitTypeDetail);

router.post('/updateDriverBaseinfo', driverService.updateDriverBaseinfo);
router.post('/updateTaskMileageInfo', driverService.updateTaskMileageInfo);
router.post('/updateTaskMileageStatus', driverService.updateTaskMileageStatus);
router.post('/getSystemGroup', driverService.getSystemGroup);

router.post('/approvePermitExchangeApply', driverService.approvePermitExchangeApply);
router.post('/downloadDriverPermitExchangeApply', driverService.downloadDriverPermitExchangeApply);

router.post('/getDriverHoto', driverService.getDriverHoto);

router.post('/reactivateDriver', driverService.reactivateDriver);

router.post('/getUserDriverSummaryList', driverService.getUserDriverSummaryList);

router.post('/approveAssessmentRecord', driverService.approveAssessmentRecord);
router.post('/approvePermitTypeDetail', driverService.approvePermitTypeDetail);
router.post('/approvePlatformConf', driverService.approvePlatformConf);

router.post('/getDriverAchievementData', driverService.getDriverAchievementData);

router.post('/getDriverEffectiveData', driverService.getDriverEffectiveData);

router.post('/addDriverPermitLog', driverService.addDriverPermitLog);
router.post('/getDriverPermitLogs', driverService.getDriverPermitLogs);

//Civilian Licence
router.post('/addCivilianLicence', driverService.addCivilianLicence);
router.post('/editCivilianLicence', driverService.editCivilianLicence);
router.post('/getCivilianLicence', driverService.getCivilianLicence);
router.post('/getCivilianLicenceById', driverService.getCivilianLicenceById);
router.post('/deleteCivilianLicenceById', driverService.deleteCivilianLicenceById);

module.exports = router;
