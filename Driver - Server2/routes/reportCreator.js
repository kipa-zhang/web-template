const express = require('express');
const router = express.Router();
require('express-async-errors');

const reportCreatorService = require('../services/reportCreatorService');

router.get('/', (req, res) => {
    res.render('reportCreator/report.html');
});

router.get('/task', (req, res) => {
    res.render('reportCreator/task.html');
});

router.get('/telematic', (req, res) => {
    res.render('reportCreator/telematic.html');
});

router.get('/obd', (req, res) => {
    res.render('reportCreator/obd.html');
});

router.get('/keypress', (req, res) => {
    res.render('reportCreator/keypress.html');
});

router.get('/driverReport', (req, res) => {
    res.render('reportCreator/driver-creator.html');
});

router.get('/vehicleReport', (req, res) => {
    res.render('reportCreator/vehicle-creator.html');
});

router.post('/getDriverReportList', reportCreatorService.getDriverReportList)
router.post('/getVehicleReportList', reportCreatorService.getVehicleReportList)
router.post('/getTaskReportList', reportCreatorService.getTaskReportList);
router.post('/getKeyPressReportList', reportCreatorService.getKeyPressReportList);
router.post('/getTelematicReportList', reportCreatorService.getTelematicReportList);
router.post('/getOBDReportList', reportCreatorService.getOBDReportList);
router.get('/downloadExcel', reportCreatorService.DownloadExcel)



module.exports = router;