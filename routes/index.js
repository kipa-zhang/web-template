const express = require('express');
// const fs = require('fs');
const fs = require('graceful-fs');
const router = express.Router();
require('express-async-errors');

const conf = require('../conf/conf');

const mobileService = require('../service/mobileService');
const driverService = require('../service/driverService');
const incidentService = require('../service/incidentService');
const positionService = require('../service/positionService');
const stateService = require('../service/stateService');
const zoneService = require('../service/zoneService');

const uploadService = require('../service/uploadService');

router.get('/', function(req, res, next) {
    return res.json({
        "respCode": 1,
        "respMessage": 'success'
    });
});

router.get('/mobileLogin', function(req, res, next) {
    res.render('loginTO', { title: 'Login' });
});

router.post('/getLatestAPKVersion', function(req, res, next) {
    return res.json({
        "respCode": 1,
        "respMessage": { latestAPKVersion: conf.LatestAPKVersion }
    });
});

router.post('/getLatestAPKVersionForIOS', function(req, res, next) {
    return res.json({
        "respCode": 1,
        "respMessage": { latestAPKVersion: conf.LatestIOSVersion }
    });
});

router.get('/downloadMMPK', function(req, res, next) {
    res.download(conf.mmpk_path);
    // let rs = fs.createReadStream(conf.mmpk_path);
    // res.writeHead(200, {
    //     'Content-Type': 'application/force-download',
    //     'Content-Disposition': 'attachment; filename=' + 'Singapore.mmpk'
    // });
    // rs.pipe(res);
});
// router.post('/downloadMMPK', function(req, res, next) {
//     res.download(conf.mmpk_path);
// });

router.post('/uploadUserIcon', uploadService.uploadUserIcon);

router.post('/updateFirebaseToken', mobileService.updateFirebaseToken);

router.post('/mobileLogin', mobileService.mobileLogin);
router.post('/mobileSingpassLogin', mobileService.mobileSingpassLogin);
router.post('/mobileLogout', mobileService.mobileLogout);
router.post('/checkUserCanAddTrip', mobileService.checkUserCanAddTrip);

router.post('/getNoGoZoneList', zoneService.getNoGoZoneList);
router.post('/getUserZoneList', zoneService.getUserZoneList);

router.post('/createIncident', incidentService.createIncident);
router.post('/getIncidentList', incidentService.getIncidentList);
router.post('/getAllIncidentList', incidentService.getAllIncidentList);
router.post('/getIncidentTypeList', incidentService.getIncidentTypeList);

router.post('/updateWaypointPosition', positionService.updateWaypointPosition);
router.post('/updatePositionByFile', positionService.updatePositionByFile);

router.post('/getFriendsPosition', positionService.getFriendsPosition);

router.post('/getStateRecord', stateService.getStateRecord);  
router.post('/getOBDStatus', stateService.getOBDStatus);  
router.post('/getLatestStateRecord', stateService.getStateRecord);  
router.post('/deleteStateRecord', stateService.deleteStateRecord);

router.post('/getDriverTask', driverService.getDriverTask);
router.post('/getNavigation', driverService.getNavigation);

router.post('/getTODriverById', driverService.getTODriverById);
router.post('/getToDriverByNRIC', driverService.getToDriverByNRIC);
router.post('/updateDriverById', driverService.updateDriverById);

router.post('/getDriverMileageStatInfo', driverService.getDriverMileageStatInfo);
router.post('/getPlatformListGroupByVehicleType', driverService.getPlatformListGroupByVehicleType);

router.post('/getPlatformConfList', driverService.getPlatformConfList);

const taskService = require('../service/taskService');
// Start Task( Update driver task status)
router.post('/startTask', taskService.startTask2);
router.post('/getWeatherForecast', taskService.getWeatherForecast);
router.post('/getDriverMileage', taskService.getDriverMileage);
router.post('/getTOIndents', taskService.getTOIndents);
router.post('/getTOIndentByTaskId', taskService.getTOIndentByTaskId);
router.post('/getTaskSummary', taskService.getTaskSummary);
router.post('/reportTaskODD', taskService.reportTaskODD);

router.post('/reportLateReason', taskService.reportLateReason);

router.post('/updateTaskOptTime', taskService.updateTaskOptTime)

router.post('/getTaskTrackingStatInfo', taskService.getTaskTrackingStatInfo);


// MT-RAC
const mtRACService = require('../service/mtRACService');
router.post('/getMT_RAC', mtRACService.getMT_RAC);
router.post('/getTransportLeader', mtRACService.getTransportLeader);
router.post('/getRiskAssessment', mtRACService.getRiskAssessment);
router.post('/getRiskAssessmentList', mtRACService.getRiskAssessmentList);
router.post('/getDriverDeclaration', mtRACService.getDriverDeclaration);
router.post('/getDriverDeclarationList', mtRACService.getDriverDeclarationList);
router.post('/getMT_RACData', mtRACService.getMT_RACData);

router.post('/createMT_RAC', mtRACService.createMT_RAC);
router.post('/verifyMT_RAC', mtRACService.verifyMT_RAC);

// route brief
// delete 2023-11-15

// vehicle
const vehicleService = require('../service/vehicleService');
router.post('/getVehicleInfo', vehicleService.GetVehicleInfo);

// incident
router.post('/getIncidentList', incidentService.getIncidentList);
router.post('/createIncident', incidentService.createIncident);
router.post('/getIncidentTypeList', incidentService.getIncidentTypeList);

router.post('/updateDriverStatus', driverService.updateDriverStatus);


// Notification
const noticeService = require('../service/noticeService');
router.post('/getNoticeList', noticeService.getNoticeList);
router.post('/getPopupNoticeList', noticeService.getPopupNoticeList);
router.post('/updatePopupNoticeAsRead', noticeService.updatePopupNoticeAsRead);


// Trip
const tripService = require('../service/tripService');
router.post('/getTripIndents', tripService.getTripIndents);
router.post('/cancelTripById', tripService.cancelTripById);

router.post('/updateDriverEmail', driverService.updateDriverEmail);
router.post('/getDriverPermitStatus', driverService.getDriverPermitStatus);
router.post('/getDriverAchievementData', driverService.getDriverAchievementData);

module.exports = router;
