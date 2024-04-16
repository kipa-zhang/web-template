const express = require('express');
const router = express.Router();
require('express-async-errors');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const conf = require('../conf/conf');
const utils = require('../util/utils');

const userService = require('../services/userService');
const urgentService = require('../services/urgentService');

const { User } = require('../model/user.js');

router.get('/', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Urgent Duty')
    res.render('urgent/index', { userId: req.cookies.userId, userType: req.cookies.userType, pageList });
});

router.get('/getForbiddenDate', urgentService.getForbiddenDate)
router.post('/getVehicleByGroupId', urgentService.getVehicleByGroupId)
router.post('/getDriverByGroupId', urgentService.getDriverByGroupId)
router.post('/createUrgentConfig', urgentService.createUrgentConfig)
router.post('/getUrgentConfig', urgentService.getUrgentConfig)
router.post('/getUrgentDutyById', urgentService.getUrgentDutyById)
router.post('/updateUrgentDutyById', urgentService.updateUrgentDutyById)
router.post('/cancelUrgentDutyById', urgentService.cancelUrgentDutyById)



router.post('/getIndentList', urgentService.getUrgentIndentList)
router.post('/cancelIndent', urgentService.cancelIndent)
router.post('/reAssignIndent', urgentService.reAssignIndent)
router.post('/getAvailableDuty', urgentService.getAvailableDutyList)

module.exports = router;
