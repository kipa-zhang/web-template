const express = require('express');
const router = express.Router();
require('express-async-errors');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const log = require('../log/winston').logger('Route Index');

const CONTENT = require('../util/content.js');
const moment = require('moment');
const conf = require('../conf/conf');
const utils = require('../util/utils');

const userService = require('../services/userService');
const groupService = require('../services/groupService');
const mileageService = require('../services/mileageService');
const resourceHomeService = require('../services/resourceHomeService');

const { Role } = require('../model/permission/role.js');
const { User } = require('../model/user.js');
const { Unit } = require('../model/unit.js');
const { Group } = require('../model/system/group.js');
const { OperationRecord } = require('../model/operationRecord.js');
const _SysUser = require('../model/system/user.js');

router.get('/', async (req, res) => {

    let { token } = req.query
    let user = null;
    if (!token) {
        // Update jumpSystemToken, ready for jump to system
        user = await User.findByPk(req.cookies.userId)
        res.cookie('jumpSystemToken', utils.generateAESCode(JSON.stringify({
            loginName: user.username,
            password: user.password
        })), { expires: utils.expiresCookieDate() });
    } else {
        // Jump from system, need update cookie, userId
        try {
            token = JSON.parse(utils.decodeAESCode(token))
            user = await User.findOne({ where: { username: token.loginName, password: token.password } })
            if (user) {
                let jwtToken = utils.generateTokenKey({ userId: user.userId });
                // Update User info
                user.online = 1;
                user.lastLoginTime = moment();
                user.jwtToken = jwtToken;
                user.save();

                let unit = await Unit.findByPk(user.unitId)

                // Store User into cookie
                res.cookie('token', jwtToken, { expires: utils.expiresCookieDate() });
                req.session.token = jwtToken;
                res.cookie('userId', user.userId, { expires: utils.expiresCookieDate() });
                req.session.userId = user.userId;
                res.cookie('username', user.username, { expires: utils.expiresCookieDate() });
                res.cookie('fullName', user.fullName, { expires: utils.expiresCookieDate() });
                res.cookie('userType', user.userType, { expires: utils.expiresCookieDate() });
                res.cookie('email', 'email', { expires: utils.expiresCookieDate() });
                res.cookie('lastLoginTime', moment().format('YYYY-MM-DD HH:mm:ss'), { expires: utils.expiresCookieDate() });
                res.cookie('needChangePassword', 0, { expires: utils.expiresCookieDate() });
                res.cookie('role', user.role, { expires: utils.expiresCookieDate() });
                res.cookie('hub', unit ? unit.unit : '', { expires: utils.expiresCookieDate() });
                res.cookie('node', (unit && unit.subUnit) ? unit.subUnit: '', { expires: utils.expiresCookieDate() });
                res.cookie('VehicleMissingFrequency', conf.VehicleMissingFrequency, { expires: utils.expiresCookieDate() });
                // Store System Conf into cookie
                res.cookie('sysConf', {
                    allow_audio_img_file: 1,
                    allow_audio_radio_call: 1,
                }, { expires: utils.expiresCookieDate() });

                let newOptRecord = {
                    operatorId: user.userId,
                    businessType: 'login_record',
                    businessId: 'laptop',
                    optType: 'system login', 
                    remarks: utils.getClientIP(req)
                }
                await OperationRecord.create(newOptRecord);

                // refresh current page, will hide params
                return res.redirect('/')
            } else {
                log.warn(`User ${ token.loginName } does not exist, will redirect to login page.`)
                return res.render('login/index', { title: 'Welcome Mobius', singpassError: '', loginError: `User ${ token.loginName } does not exist, please select another account.` });
            }   
        } catch (error) {
            log.error(error)
            return res.render('login/index', { title: 'Welcome Mobius', singpassError: '', loginError: '' });
        }
        
    }

    let pageList = await userService.getUserPageList(user.userId)

    let availableSwitchCV = false;
    let baseUser = await sequelizeObj.query(` SELECT * FROM user_base WHERE mvUserId = ? `, { type: QueryTypes.SELECT, replacements: [ user.userId ] })
    if (baseUser.length && baseUser[0].cvUserId) {
        // check cv user is not lock out and deactived
        let cvUser = await _SysUser.USER.findByPk(baseUser[0].cvUserId);
        if (cvUser && cvUser.status != CONTENT.CV_USER_STATUS.Deactivated && cvUser.status != CONTENT.CV_USER_STATUS.LockOut) {
            availableSwitchCV = true;
        }
    }
    
    res.cookie('userLocalMapTile', conf.Use_Local_MapTile, { expires: utils.expiresCookieDate() });

    utils.generateMapCookie(pageList, res)

    res.cookie('userLocalMapTile', conf.Use_Local_MapTile, { expires: utils.expiresCookieDate() });
    res.cookie('systemServer', conf.systemServer, { expires: utils.expiresCookieDate() });


    res.render('index', { title: 'Driver', userId: user.userId, userType: user.userType, pageList, availableSwitchCV });
});


router.get('/login', (req, res) => res.render('login/index', { title: 'Welcome Mobius', singpassError: '', loginError: '' }));
router.get('/guide', (req, res) => res.render('guide', { title: 'Welcome Mobius'}));
router.post('/login', userService.login);
router.post('/logout', userService.logout);

router.post('/loginUseSingpass', userService.loginUseSingpass);

router.post('/getRoleVocation', userService.getRoleVocation);

router.post('/getUnitList', userService.getUnitList);
router.post('/getUnit', userService.getUnit);
router.post('/createUnit', userService.createUnit);
router.post('/updateUnit', userService.updateUnit);
router.post('/deleteUnit', userService.deleteUnit);

router.post('/getUserList', userService.getUserList);
router.post('/getCurrentUser', userService.getCurrentUser);
router.post('/createUser', userService.createUser);
router.post('/updateUser', userService.updateUser);
router.post('/deleteUser', userService.deleteUser);
router.post('/enableUser', userService.enableUser);

router.post('/getUserTypeList', userService.getUserTypeList);
router.post('/getLaptopUserList', userService.getLaptopUserList);
router.post('/getMobileUser', userService.getMobileUser);
router.post('/getMobileUserList', userService.getMobileUserList);
router.post('/deleteMobileUser', userService.deleteMobileUser);

router.post('/getUserGroupList', groupService.getUserGroupList);
router.post('/getUserListWithNoGroup', groupService.getUserListWithNoGroup);
router.post('/createUserGroup', groupService.createUserGroup);
router.post('/updateUserGroup', groupService.updateUserGroup);

router.post('/getMileageList', mileageService.InitMileageList)

router.post('/getResourcesStatData', resourceHomeService.getResourcesStatData)

router.get('/hq', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Task Assign')

    res.render('hq/hq', { pageList });
});
router.get('/resources', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Resources')
    res.render('resources/resources', { pageList });
});

router.get('/keyManagement', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Mustering');
    res.render('ekeypress/keyManagement', { title: 'Keypress Management', pageList });
});

router.get('/scanEkeypressQRCode', (req, res) => res.render('ekeypress/scanQrCode', { title: 'Scan QRCode'}));

router.get('/addKeyOptRecord', (req, res) => res.render('ekeypress/addKeyOptRecord', { title: 'Key Option Record'}));

router.get('/licensing', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Resources', 'Licensing');
    let operationList = pageList ? pageList.map(item => `${ item.action }`) : [];

    res.render('licensing/licensing', {operationList});
});
router.get('/vehicleType', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Resources', 'Vehicle Type');
    let operationList = pageList ? pageList.map(item => `${ item.action }`) : [];

    res.render('vehicle/manageVehicleType', {operationList});
});
router.get('/resources/paradeState', (req, res) => {
    res.render('resources/paradeState');
});
router.get('/mtAdmin', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'MT-Admin')
    res.render('mtAdmin/mtAdmin', { pageList });
});
router.get('/dashboard', async (req, res) => {
    let user = await User.findByPk(req.cookies.userId)
    let hubNodeInfo = '-'
    if (user.unitId) {
        if (user.userType.toLowerCase() == 'customer') {
            let group = await Group.findByPk(user.unitId)
            hubNodeInfo = group.groupName
        } else {
            let unit = await Unit.findByPk(user.unitId)
            hubNodeInfo = `${ unit.unit }/${ unit.subUnit ? unit.subUnit : '-' }`
        }
    } else if (user.userType.toLowerCase() == 'administrator') {
        hubNodeInfo = 'Administrator'
    } else if (user.userType.toLowerCase() == 'hq') {
        hubNodeInfo = 'HQ'
    } 

    res.render('dashboard/dashboard', { userType: req.cookies.userType, fullName: user.fullName, hubNodeInfo, lastLoginTime: moment(user.lastLoginTime).format('YYYY-MM-DD HH:mm:ss') });
});
router.get('/MV-Dashboard', async (req, res) => {

    let pageList = await userService.getUserPageList(req.cookies.userId)
    
    utils.generateMapCookie(pageList, res)
    
    res.render('task/mvAndMtTotal', { userType: req.cookies.userType });
});
router.get('/hoto', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'HOTO')
    res.render('hotoManagement/hotoManagement', { pageList });
});

router.get('/resourcesDashboard', (req, res) => {
    res.render('resourcesDashboard/resourcesDashboard');
});
router.get('/resourcesDashboard2', (req, res) => {
    res.render('resourcesDashboard/resourcesDashboard2');
});

const locationService = require('../services/locationService');
router.post('/getLocationList', locationService.getLocationList)
router.post('/updateLocation', locationService.updateLocation)
router.post('/deleteLocation', locationService.deleteLocation)
router.post('/createLocation', locationService.createLocation)

const mobileTaskService = require('../services/mobileTaskService');
router.post('/approveMobileTaskById', mobileTaskService.approveMobileTaskById)
router.post('/cancelMobileTaskById', mobileTaskService.cancelMobileTaskById)
router.post('/getDVLOATaskVehicle', mobileTaskService.getDVLOATaskVehicle)
router.post('/editMobileTask', mobileTaskService.editMobileTask)

// SOS
const sosService = require('../services/sosService');
router.get('/sos', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'SOS')
    res.render('sos/sos', { userType: req.cookies.userType, pageList });
});
router.post('/getSOSList', sosService.getSOSList)
router.post('/updateSOS', sosService.updateSOS)

router.post('/getGroupList', groupService.getGroupList)

// Report
const reportService = require('../services/reportService');
router.post('/report', reportService.report)
router.get('/report', (req, res) => {
    res.render('report/view');
});

router.get('/arbReport', (req, res) => {
    res.render('report/arbReport');
});
router.post('/arbReportByHubNode', reportService.arbReportByHubNode)
router.post('/arbReportByTO', reportService.arbReportByTO)

router.get('/utilisationReport', (req, res) => {
    res.render('report/utilisationReport.html');
});
router.post('/resourceMonthUtilisationReport', reportService.resourceMonthUtilisationReport);

router.get('/licensingReport', (req, res) => {
    res.render('report/licensingReport.html');
});
router.post('/licensingMonthReport', reportService.licensingMonthReport);

router.get('/opsSummary', (req, res) => {
    res.render('report/opsSummary.html');
});
router.post('/opsSummaryReport', reportService.opsSummaryReport);

// Role
const roleService = require('../services/roleService');
router.post('/role/create', roleService.updateRole)
router.post('/role/update', roleService.updateRole)
router.post('/role/delete', roleService.deleteRole)
router.post('/role/getRoleList', roleService.getRoleList)
router.post('/role/getPageList', roleService.getPageList)
router.get('/role', async (req, res) => {
    let userId = req.cookies.userId
    let pageList = await userService.getUserPageList(userId, 'Role Management')
    res.render('role/role-view', { userType: req.cookies.userType, pageList });
});

// config
const configService = require('../services/configService');
router.post('/getHubConf', configService.getHubConf)
router.post('/updateHubConf', configService.updateHubConf)

module.exports = router;
