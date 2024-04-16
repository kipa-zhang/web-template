const express = require('express');
const router = express.Router();
require('express-async-errors');

router.get('/overview', (req, res) => {
    res.render('dashboard/overview');
});

router.get('/credit', (req, res) => {
    res.render('dashboard/credit');
});

router.get('/mvAndMtTotal', (req, res) => {
    res.render('dashboard/mvAndMtTotal');
});

const creditService = require('../services/creditService');
const taskService = require('../services/taskService');
const userService = require('../services/userService');

// router.post('/getDriverAndVehicleDeployedTotal', taskService.getDriverAndVehicleDeployedTotal);

router.get('/getPurposeTypeList', creditService.getPurposeTypeList);
router.post('/getCreditInfo', creditService.getCreditInfo);
router.post('/getCreditInfoByYear', creditService.getCreditInfoByYear);

router.post('/getPurposeList', taskService.getPurposeList)
router.post('/getHubByPurpose', taskService.getHubByPurpose2)
router.post('/getNodeByPurpose', taskService.getNodeByPurpose2)

router.post('/getTodayOffenceList', taskService.getTodayOffenceList)
router.post('/getAllOffenceDashboard', taskService.getAllOffenceDashboard)
router.post('/getTodayOffenceDashboard', taskService.getTodayOffenceDashboard)
router.post('/getTodayInTaskVehicleList', taskService.getTodayInTaskVehicleList)
router.post('/getDriverStateSos', taskService.getDriverStateSos)
router.post('/getTrafficList', taskService.getTrafficList)

router.post('/getTodayRealSpeeding', taskService.getTodayRealSpeeding)

router.post('/getTodayRealAlert', taskService.getTodayRealAlert)

router.get('/task', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Task Dashboard')

    res.render('dashboard/background', { userType: req.cookies.userType , pageList});
});

router.get('/withoutVehicleTaskList', async (req, res) => {
    res.render('task/withoutVehicleTaskList', { userType: req.cookies.userType });
});

router.post('/getTaskList', taskService.getTaskList)
router.post('/getWithoutVehicleTaskNum', taskService.getWithoutVehicleTaskNum);
router.post('/getWithoutVehicleTaskList', taskService.getWithoutVehicleTaskList);
router.post('/getDriverMobileTaskList', taskService.getDriverMobileTaskList)
router.post('/getMT_RACList', taskService.getMT_RACList)
router.post('/deleteMT_RAC', taskService.deleteMT_RAC)
router.post('/getODDList', taskService.getOddList)
router.post('/getSurveyList', taskService.getSurveyList)
router.post('/getIncidentList', taskService.getIncidentList)

router.post('/getATMSLoanTaskList', taskService.getATMSLoanTaskList)
router.post('/getCVLoanTaskList', taskService.getCVLoanTaskList)
router.post('/startLoan', taskService.startLoan)
router.post('/completeLoan', taskService.completeLoan)

module.exports = router;
