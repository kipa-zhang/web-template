const log = require('../log/winston').logger('Login Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system');

const { User } = require('../model/user');
const { Friend } = require('../model/friend');
const { UserGroup } = require('../model/userGroup');
const { Room } = require('../model/room');
const { RoomMember } = require('../model/roomMember');
const { UserNotice } = require('../model/userNotice');
const { UserZone } = require('../model/userZone.js');
const { Driver } = require('../model/driver.js');
const { DriverPosition } = require('../model/driverPosition.js');
const { SystemConf } = require('../model/systemConf.js');
const { Unit } = require('../model/unit.js');
const { ModulePage } = require('../model/permission/modulePage');

const userService = require('../services/userService');
const groupService = require('../services/groupService');
const unitService = require('../services/unitService');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { DriverTask } = require('../model/driverTask.js');
const { OperationRecord } = require('../model/operationRecord.js');
const { UserBase } = require('../model/userBase.js');
const _SysUser = require('../model/system/user.js');

const roleVocation = {
    "TO": [ "BASE", "BASE (Trainee)", "CBT", "CBT (Trainee)", "CS/CSS", "CS/CSS (Trainee)", "SVC", "SVC (Trainee)"
    , "STO", "STO (Trainee)", "COXSWAIN", "COXSWAIN (Trainee)", "-" ],
    "TL": [ "ADV", "Trainee", "Basic", "-" ],
    "DV": [ "-" ],
    "LOA": [ "-" ]
}

const JUST_HQ_APPROVE_ROLE = conf.HQ_Approve_Mvuser_Role ? conf.HQ_Approve_Mvuser_Role.split(',') : [];

let TaskUtils = {
    getGroupById: async function (id) {
        return await sequelizeSystemObj.transaction(async t => {
            let group = await sequelizeSystemObj.query(
                `select id, groupName from \`group\` where id = ?`,
                {
                    type: QueryTypes.SELECT
                    , replacements: [id]
                }
            );
            return group[0]
        })
    },
    initCVAndMVUser: async function(accountUser, operationType, userBaseId, loginUserId, loginUserType){
        const mvAffair = await sequelizeObj.transaction();
        const cvAffair = await sequelizeSystemObj.transaction();
        try{
            let cvPassword = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toLowerCase();
            let mvPassword = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toUpperCase();
            accountUser.nric = utils.generateAESCode(accountUser.nric).toUpperCase();
            let mvUser = null;
            let cvUser = null;
            let newUserBase = null
            let oldUserBase = null;
            let oldCVUser = null;
            let oldMVUser = null;
            if(userBaseId) {
                oldUserBase = await UserBase.findOne({ where: { id: userBaseId } })
                if(oldUserBase.cvUserId) {
                    oldCVUser = await _SysUser.USER.findOne({ where: { id: oldUserBase.cvUserId } })
                }
                if(oldUserBase.mvUserId) {
                    oldMVUser = await User.findOne({ where: { userId: oldUserBase.mvUserId } })
                }
            }
        
            if(oldUserBase) {
                //Reapply for a cv user
                if(operationType.toLowerCase() == 'homeregistration' && oldUserBase.cvRejectDate && accountUser.cvRole) {
                    await UserBase.update({ cvRejectDate: null, cvRejectBy: null, cvRejectReason: null }, { where: { id: userBaseId }, transaction: mvAffair });
                }
                //Reapply for a mv user
                if(operationType.toLowerCase() == 'homeregistration' && oldUserBase.rejectDate && accountUser.mvUserType) {
                    await UserBase.update({ status: 'Pending Approval', rejectDate: null, rejectBy: null, rejectReason: null }, { where: { id: userBaseId }, transaction: mvAffair });
                }
            }
            let cvmvUserBase = null;
            if(userBaseId) cvmvUserBase = await UserBase.findOne({ where: { id: userBaseId } })
            let modelPageList = await userService.getUserPageList(loginUserId, 'User Management', 'View User')
            let operationList = modelPageList.map(item => `${ item.action }`).join(',')
            operationList = operationList.split(',')
            // let mvState = operationType.toLowerCase() == 'edit' ? oldUserBase.approveDate ? true : false : true
            // let cvState = operationType.toLowerCase() == 'edit' ? oldUserBase.cvApproveDate ? true : false : true
            let mvRoleApprovalState = true;
            if(accountUser.mvRoleName && operationType.toLowerCase() == 'edit') {
                if(JUST_HQ_APPROVE_ROLE.includes(accountUser.mvRoleName) && loginUserType.toLowerCase() != 'hq') mvRoleApprovalState = false;
            }
            let mvState = operationType.toLowerCase() == 'homeregistration' ? oldUserBase.approveDate ? true : false : true
            let cvState = operationType.toLowerCase() == 'homeregistration' ? oldUserBase.cvApproveDate ? true : false : true
            let mvEditApprovalState = cvmvUserBase ? !cvmvUserBase.rejectDate : true && mvRoleApprovalState && operationList.includes("Approval Status");
            let mvState2 = operationType.toLowerCase() == 'edit' ? mvEditApprovalState ? true : false : true;
            let cvEditApprovalState = cvmvUserBase ? !cvmvUserBase.cvRejectDate : true && operationList.includes("Approval Status");
            let cvState2 = operationType.toLowerCase() == 'edit' ? cvEditApprovalState ? true : false : true
            if(mvState && mvState2) {
                if(accountUser.mvUserType) {
                    let unitId = accountUser.mvUnitId ? accountUser.mvUnitId : accountUser.mvGroupId;
                    if(accountUser.mvUserType.toLowerCase() == 'hq') unitId = null
                    if(oldMVUser){
                        await User.update({
                            username: accountUser.loginName,
                            fullName: accountUser.fullName,
                            nric: accountUser.nric,
                            unitId: unitId,
                            // unitId: accountUser.mvUnitId ? accountUser.mvUnitId : accountUser.mvGroupId,
                            // password: mvPassword,
                            ord: accountUser.ord,
                            userType: accountUser.mvUserType,
                            role: accountUser.mvRoleName, 
                            contactNumber: accountUser.contactNumber,
                            hq: accountUser.hq
                        }, { where: { userId: oldMVUser.userId }, transaction: mvAffair })
                        mvUser = { userId: oldMVUser.userId }
                    } else {
                        mvUser = await User.create({
                            username: accountUser.loginName,
                            fullName: accountUser.fullName,
                            nric: accountUser.nric,
                            ord: accountUser.ord,
                            unitId: unitId,
                            // unitId: accountUser.mvUnitId ? accountUser.mvUnitId : accountUser.mvGroupId,
                            password: mvPassword,
                            userType: accountUser.mvUserType,
                            role: accountUser.mvRoleName,
                            contactNumber: accountUser.contactNumber,
                            hq: accountUser.hq
                        }, { transaction: mvAffair })
                    }
                } else {
                    if(oldMVUser) {
                        await User.destroy({ where: { userId: oldMVUser.userId }, transaction: mvAffair });
                    }
                }
            }
            if(cvState && cvState2) {
                if(accountUser.cvRole){
                    if(oldCVUser){
                       await _SysUser.USER.update({
                            nric: accountUser.nric,
                            username: accountUser.fullName.toUpperCase(),
                            loginName: accountUser.loginName,
                            role: accountUser.cvRole,
                            ord: accountUser.ord,
                            // password: cvPassword,
                            // historyPassword: cvPassword,
                            group: accountUser.cvGroupId,
                            contactNumber: accountUser.contactNumber,
                            email: accountUser.email,
                            serviceProviderId: accountUser.cvServiceProviderId,
                            serviceTypeId: accountUser.cvServiceTypeId,
                        }, { where: { id: oldCVUser.id }, transaction: cvAffair })
                        cvUser = { id: oldCVUser.id }
                    } else {
                        cvUser = await _SysUser.USER.create({
                            nric: accountUser.nric,
                            username: accountUser.fullName.toUpperCase(),
                            loginName: accountUser.loginName,
                            ord: accountUser.ord,
                            role: accountUser.cvRole,
                            password: cvPassword,
                            historyPassword: cvPassword,
                            group: accountUser.cvGroupId,
                            contactNumber: accountUser.contactNumber,
                            email: accountUser.email,
                            serviceProviderId: accountUser.cvServiceProviderId,
                            serviceTypeId: accountUser.cvServiceTypeId,
                        }, { transaction: cvAffair })
                    }
                } else {
                    if(oldCVUser) {
                        await _SysUser.USER.destroy({ where: { id: oldCVUser.id }, transaction: cvAffair });
                    }
                }
            }

            let cvUserId = cvUser ? cvUser.id : null;
            let mvUserId = mvUser ? mvUser.userId : null;
            if(!userBaseId) {
                let createUserBaer = {
                    cvUserId: cvUserId,
                    mvUserId: mvUserId,
                    ord: accountUser.ord,
                    nric: accountUser.nric,
                    fullName: accountUser.fullName,
                    loginName: accountUser.loginName,
                    password: mvPassword,
                    contactNumber: accountUser.contactNumber,
                    email: accountUser.email,
                    status: 'Approved',
                    // cvApproveDate: accountUser.approveDate,
                    // cvApproveBy: 0,
                    cvRole: accountUser.cvRole,
                    cvGroupId: accountUser.cvGroupId,
                    cvGroupName: accountUser.cvGroupName,
                    cvServiceProviderId: accountUser.cvServiceProviderId,
                    cvServiceTypeId: accountUser.cvServiceTypeId,
                    mvUserType: accountUser.mvUserType,
                    mvUnitId: accountUser.mvUnitId,
                    mvGroupId: accountUser.mvGroupId,
                    mvGroupName: accountUser.mvGroupName,
                    mvRoleName: accountUser.mvRoleName,
                    dataFrom: accountUser.dataFrom,
                    creator: accountUser.creator,
                    hq: accountUser.hq
                    // approveDate: accountUser.approveDate,
                    // approveBy: 0
                }
                if(accountUser.cvRole){
                    createUserBaer.cvApproveDate = accountUser.approveDate
                    createUserBaer.cvApproveBy = 0
                }
                if(accountUser.mvUserType) {
                    createUserBaer.approveDate = accountUser.approveDate,
                    createUserBaer.approveBy = 0
                }
                newUserBase = await UserBase.create(createUserBaer, { transaction: mvAffair })

            } else {
                if(operationType.toLowerCase() != 'create') {
                    let editUserBase = {
                        cvUserId: cvUserId,
                        mvUserId: mvUserId,
                        ord: accountUser.ord,
                        nric: accountUser.nric,
                        fullName: accountUser.fullName,
                        loginName: accountUser.loginName,
                        // password: mvPassword,
                        contactNumber: accountUser.contactNumber,
                        email: accountUser.email,
                        cvRole: accountUser.cvRole,
                        cvGroupId: accountUser.cvGroupId,
                        cvGroupName: accountUser.cvGroupName,
                        cvServiceProviderId: accountUser.cvServiceProviderId,
                        cvServiceTypeId: accountUser.cvServiceTypeId,
                        mvUserType: accountUser.mvUserType,
                        mvUnitId: accountUser.mvUnitId,
                        mvGroupId: accountUser.mvGroupId,
                        mvGroupName: accountUser.mvGroupName,
                        mvRoleName: accountUser.mvRoleName,
                        hq: accountUser.hq
                    }
                    if(oldUserBase){
                        if(!oldUserBase.approveDate && editUserBase.mvUserType && operationType.toLowerCase() != 'homeregistration' && !oldUserBase.rejectDate){
                            editUserBase.approveDate = moment().format('YYYY-MM-DD HH:mm:ss')
                            editUserBase.approveBy = 0
                        }
                        if(!oldUserBase.cvApproveDate && editUserBase.cvRole && operationType.toLowerCase() != 'homeregistration' && !oldUserBase.cvRejectDate){
                            editUserBase.cvApproveDate = moment().format('YYYY-MM-DD HH:mm:ss')
                            editUserBase.cvApproveBy = 0
                        }
                        // 2024-01-24 old password is null 
                        if(!oldUserBase.password){
                            let oldUserPassword = null;
                            if(oldUserBase.cvUserId) {
                                let oldSysUser = await _SysUser.USER.findOne({ where: { id: oldUserBase.cvUserId } })
                                oldUserPassword = oldSysUser ? oldSysUser.password.toUpperCase() : null;
                            }
                            if(oldUserBase.mvUserId) {
                                let oldServerUser = await User.findOne({ where: { userId: oldUserBase.mvUserId } })
                                oldUserPassword = oldServerUser ? oldServerUser.password : null;
                            }
                            editUserBase.password = oldUserPassword ? oldUserPassword : mvPassword;
                        }
                    }
                    await UserBase.update(editUserBase, { where: { id: userBaseId }, transaction: mvAffair });
                }
            }
            if(!newUserBase) newUserBase = await UserBase.findOne({ where: { id: userBaseId }, transaction: mvAffair })
            
            if(oldUserBase || newUserBase){
                let operatorId = accountUser.creator;
                let loginUser = null;
                if(accountUser.creator) loginUser = await User.findOne({ where: { userId: accountUser.creator }, transaction: mvAffair })
                let optType = oldUserBase ? 'Account Edit' : 'Account Creation';
                // let dataType = null;
                if(operationType.toLowerCase() == 'homeregistration'){
                    if(oldUserBase.cvUserId && !oldUserBase.mvUserId && newUserBase.mvUserType){
                        operatorId = oldUserBase.cvUserId;
                        loginUser = await UserBase.findOne({ where: { cvUserId: oldUserBase.cvUserId }, transaction: mvAffair })
                        // dataType = 'MV';
                        optType = 'Register MV Account'
                    } else if(!oldUserBase.cvUserId && oldUserBase.mvUserId && newUserBase.cvRole) {
                        operatorId = oldUserBase.mvUserId;
                        loginUser = await UserBase.findOne({ where: { mvUserId: oldUserBase.mvUserId }, transaction: mvAffair })
                        // dataType = 'CV';
                        optType = 'Register CV Account'
                    }
                } 
                
                if(optType.toLowerCase() != 'register mv account'){
                    await OperationRecord.create({
                        id: null,
                        operatorId: operatorId,
                        operatorName: loginUser.fullName,
                        businessType: 'Manage User',
                        businessId: oldUserBase ? oldUserBase.id : newUserBase.id,
                        optType: optType,
                        beforeData: oldUserBase ? `${ JSON.stringify(oldUserBase) }` : null,
                        afterData: `${ JSON.stringify(newUserBase) }`,
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: oldUserBase ? oldMVUser && !mvUser ? 'edit user (The user on the server side is not required)' : 'edit user' : `add user`
                    }, { transaction: mvAffair })
                }
               
                if(oldCVUser || cvUser){
                    // let cvUserBase = loginUser;
                    // if(accountUser.creator) cvUserBase = await UserBase.findOne({ where: { mvUserId: accountUser.creator }, transaction: mvAffair })
                    // let newCvUser = await _SysUser.USER.findOne({ where: { id: cvUser.id } })
                    await _SysUser.UserManagementReport.create({
                        operatorUserBaseId: loginUser ? loginUser.id : newUserBase.id,
                        triggeredBy: loginUser.fullName,
                        userId: oldCVUser ? oldCVUser.id : cvUser.id,
                        activity: optType,
                        beforeData: oldUserBase ? `${ JSON.stringify(oldUserBase) }` : null,
                        afterData: `${ JSON.stringify(newUserBase) }`,
                        operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remark: `Server ${ oldUserBase ? oldCVUser && !cvUser ? `cancel system side` : `edit` : 'create' } user:${ newUserBase ? newUserBase.fullName : oldUserBase.fullName }.`
                    }, { transaction: cvAffair })
                }
                if(operationType.toLowerCase() == 'homeregistration'){
                    if(!oldUserBase.cvUserId && newUserBase.cvRole && !newUserBase.cvRejectDate){
                        // let cvUserBase = loginUser;
                        // if(accountUser.creator) cvUserBase = await UserBase.findOne({ where: { mvUserId: accountUser.creator }, transaction: mvAffair })
                        await _SysUser.UserManagementReport.create({
                            operatorUserBaseId: loginUser ? loginUser.id : newUserBase.id,
                            triggeredBy: loginUser.fullName,
                            activity: 'Account Register',
                            afterData: `${ JSON.stringify(newUserBase) }`,
                            operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                            remark:  `Account Register ${ accountUser.dataFrom }`
                        }, { transaction: cvAffair })
                    } else if(!oldUserBase.mvUserId && newUserBase.mvUserType && !newUserBase.rejectDate){
                        await OperationRecord.create({
                            id: null,
                            operatorId: operatorId,
                            businessType: 'Manage User',
                            operatorName: loginUser.fullName,
                            businessId: newUserBase.id,
                            optType: 'Account Register',
                            afterData: `${ JSON.stringify(newUserBase) }`,
                            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                            remarks:  `Account Register ${ accountUser.dataFrom }`
                        }, { transaction: mvAffair })
                    } 
                    
                }
            }
            await cvAffair.commit();
            await mvAffair.commit();
        } catch(error) {
            await cvAffair.rollback();
            await mvAffair.rollback();
            log.error(error)
            return error
        }
        
    },
    getUserByData: async function(userBaseId, nric, contactNumber, loginName, fullName){
        nric = utils.generateAESCode(nric).toUpperCase();
        let userBase = null;
        let dataList = [];
        if(userBaseId) userBase = await UserBase.findOne({ where: { id: userBaseId } })
        if(nric){
            if(userBase) {
                if(userBase.cvUserId){
                    dataList = await _SysUser.USER.findAll({ where: { id: { [Op.ne]: userBase.cvUserId }, nric: nric } })
                    if(dataList.length > 0) {
                        return `This Nric already exists.`
                    }
                }
                if(userBase.mvUserId){
                    dataList = await User.findAll({ where: { userId: { [Op.ne]: userBase.mvUserId }, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE }, nric: nric } })
                    if(dataList.length > 0) {
                        return `This Nric already exists.`
                    }
                }
                dataList = await UserBase.findAll({ where: { id: { [Op.ne]: userBaseId }, status: { [Op.ne]: 'Rejected' }, nric: nric } })
                if(dataList.length > 0) {
                    return `This Nric already exists.`
                }
            } else {
                dataList = await _SysUser.USER.findAll({ where: { nric: nric } })
                if(dataList.length > 0) {
                    return `This Nric already exists.`
                }
                dataList = await User.findAll({ where: { nric: nric, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE } } })
                if(dataList.length > 0) {
                    return `This Nric already exists.`
                }
                dataList = await UserBase.findAll({ where: { status: { [Op.ne]: 'Rejected' }, nric: nric } })
                if(dataList.length > 0) {
                    return `This Nric already exists.`
                }
            }
        }

        if(contactNumber){
            if(userBase) {
                if(userBase.cvUserId){
                    dataList = await _SysUser.USER.findAll({ where: { id: { [Op.ne]: userBase.cvUserId }, contactNumber: contactNumber } })
                    if(dataList.length > 0) {
                        return `This Mobile Number already exists.`
                    }
                }
                if(userBase.mvUserId){
                    dataList = await User.findAll({ where: { userId: { [Op.ne]: userBase.mvUserId }, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE }, contactNumber: contactNumber } })
                    if(dataList.length > 0) {
                        return `This Mobile Number already exists.`
                    }
                }
                dataList = await UserBase.findAll({ where: { id: { [Op.ne]: userBaseId }, status: { [Op.ne]: 'Rejected' }, contactNumber: contactNumber } })
                if(dataList.length > 0) {
                    return `This Mobile Number already exists.`
                }
            } else {
                dataList = await _SysUser.USER.findAll({ where: { contactNumber: contactNumber } })
                if(dataList.length > 0) {
                    return `This Mobile Number already exists.`
                }
                dataList = await User.findAll({ where: { contactNumber: contactNumber, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE } } })
                if(dataList.length > 0) {
                    return `This Mobile Number already exists.`
                }
                dataList = await UserBase.findAll({ where: { status: { [Op.ne]: 'Rejected' }, contactNumber: contactNumber } })
                if(dataList.length > 0) {
                    return `This Mobile Number already exists.`
                }
            }
        }

        if(loginName && fullName) {
            if(userBase) {
                if(userBase.cvUserId){
                    dataList = await _SysUser.USER.findAll({ where: { id: { [Op.ne]: userBase.cvUserId }, loginName: loginName, userName: fullName } })
                    if(dataList.length > 0){
                        return ` This loginName / fullName already exists.`
                    }
                }
                if(userBase.mvUserId){
                    dataList = await User.findAll({ where: { userId: { [Op.ne]: userBase.mvUserId }, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE }, username: loginName, fullName: fullName } })
                    if(dataList.length > 0){
                        return ` This loginName / fullName already exists.`
                    }
                }
                dataList = await UserBase.findAll({ where: { id: { [Op.ne]: userBaseId }, status: { [Op.ne]: 'Rejected' }, loginName: loginName, fullName: fullName } })
                if(dataList.length > 0) {
                    return ` This loginName / fullName already exists.`
                }
            } else {
                dataList = await _SysUser.USER.findAll({ where: { loginName: loginName, userName: fullName } })
                if(dataList.length > 0){
                    return ` This loginName / fullName already exists.`
                }
                dataList = await User.findAll({ where: { username: loginName, fullName: fullName, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE } } })
                if(dataList.length > 0){
                    return ` This loginName / fullName already exists.`
                }
                dataList = await UserBase.findAll({ where: { status: { [Op.ne]: 'Rejected' }, loginName: loginName, fullName: fullName } })
                if(dataList.length > 0) {
                    return ` This loginName / fullName already exists.`
                }
            }
        }
        return null
    },
}

const getMobileUserList = async function (option = {}) {
    // If need history info, use api getDriverInfo from driverService
    try {
        let baseSQL = `
            SELECT us.userId, us.username, us.driverId, us.fullName, us.userType, us.enable,
            IF(hh.id IS NOT NULL, hh.unitId, u.id) AS unitId,
            IF(hh.id IS NOT NULL, hh.toHub, u.unit) AS hub,
            IF(hh.id IS NOT NULL, hh.toNode, u.subUnit) AS node,
            ll.id AS loanId,
            IF(ll.id IS NOT NULL, ll.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL)) AS groupId
            
            FROM \`user\` us
            LEFT JOIN driver d ON d.driverId = us.driverId
            LEFT JOIN unit u ON u.id = us.unitId
            LEFT JOIN (
                SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                FROM hoto ho 
                WHERE ho.status = 'Approved' and (NOW() BETWEEN ho.startDateTime AND ho.endDateTime)
            ) hh ON hh.driverId = d.driverId
            LEFT JOIN (
                SELECT lo.id, lo.driverId, lo.groupId 
                FROM loan lo 
                WHERE NOW() BETWEEN lo.startDate AND lo.endDate 
            ) ll ON ll.driverId = d.driverId

            WHERE us.userType = 'MOBILE'
        `
        let sql = ` SELECT uu.* FROM (${ baseSQL }) uu WHERE 1=1 `
        let replacements = []
        // Default search ignore group
        if (option.groupId == null || option.groupId == undefined) option.groupId = -1;
        if (option.groupId > 0) {
            sql += ` AND uu.groupId = ? `
            replacements.push(option.groupId)
        } else if (option.groupId == 0) {
            // No limit

        } else if (option.groupId < 0) {
            // ignore group driver
            sql += ` AND uu.groupId IS NULL `
        }

        if (option.unitId?.length) {
            sql += ` AND uu.unitId IN (?) `
            replacements.push(option.unitId)
        } else {
            sql += ` AND 1=2 `
        }

        if (option.enable) {
            sql += ` AND uu.enable = ? `
            replacements.push(option.enable)
        }
        
        if (option.username) {
            sql += ` AND uu.fullName like ? `
            replacements.push('%'+ option.username +'%')
        }

        let mobileUserList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return mobileUserList
    } catch (error) {
        throw error
    }
}

module.exports.UserUtils = {
    getLaptopUserDetailInfo: async function (userId) {
        try {
            let userList = await sequelizeObj.query(`
                    SELECT us.userId, us.username, us.userType, us.unitId, un.unit as hub, un.subUnit as node
                    FROM \`user\` us
                    LEFT JOIN unit un ON un.id = us.unitId
                    WHERE us.userId = ? LIMIT 1
                `, { type: QueryTypes.SELECT, replacements: [ userId ] })
            if (!userList.length) {
                return null;
            } else {
                return userList[0];
            }
        } catch (error) {
            throw error
        }
    },
    getUserDetailInfo: async function (userId) {
        // Laptop user API, ignore hoto code
        try {
            let userList = await sequelizeObj.query(`
                    SELECT us.userId, us.username, us.userType, us.driverId, us.hq,
                    us.unitId, un.unit, un.subUnit, un.unit as hub, un.subUnit as node
                    FROM \`user\` us
                    LEFT JOIN unit un ON un.id = us.unitId
                    WHERE us.userId = ? limit 1
                `, { type: QueryTypes.SELECT, replacements: [ userId ] })
            if (!userList.length) {
                return null;
            } else {
                let user = userList[0]
                if (user.hq) {
                    let groupResult = await sequelizeObj.query(`
                        SELECT * 
                        FROM unit 
                        WHERE hq = ?
                        AND \`group\` IS NOT NULL
                    `, { type: QueryTypes.SELECT, replacements: [ user.hq ] })
    
                    user.group = [];
                    groupResult.map(item => {
                        let groupList = item.group.split(',').map(item2 => item2.trim())
                        user.group = user.group.concat(groupList)
                    })
                    user.group = Array.from(new Set(user.group))
                } else {
                    user.group = null
                }
                return user;
            }
        } catch (error) {
            throw error
        }
    },
    checkUserExist: async function (userId) {
        try {
            return await getUserDetailInfo(userId);
        } catch (error) {
            throw error
        }
    },
    getCurrentDriverSQL: function () {
        // No used yet
        return `
            SELECT us.userId, us.username, us.fullName AS driverName, us.userType, us.driverId,
            IF(hh.toHub is NULL, un.id, hh.unitId) as unitId,
            IF(hh.toHub is NULL, un.unit, hh.toHub) as unit,
            IF(hh.toHub is NULL, un.subUnit, hh.toNode) as subUnit,
            IF(hh.toHub is NULL, un.unit, hh.toHub) as hub,
            IF(hh.toHub is NULL, un.subUnit, hh.toNode) as node
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
            LEFT JOIN (
                select ho.driverId, ho.toHub, ho.toNode, ho.unitId 
                from hoto ho 
                where (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.driverId = us.driverId
            WHERE us.userType = 'MOBILE'
        `
    },
    getMobileUserList,
}

module.exports.login = async function (req, res) {
    try {
        let username = req.body.username;
        let password = req.body.password;
        log.info('(Login)=> username: ', username);
        log.info('(Login)=> password: ', password);

        // let md5Password = password ? utils.generateMD5Code(password).toUpperCase() : '';
        let md5Password = password;

        let user = await User.findOne({
            where: {
                username,
                password: md5Password,
                userType: {
                    [Op.ne]: CONTENT.USER_TYPE.MOBILE
                },
            }
        })
        let userBase = null;
        if (user) {
            if (user.userType === CONTENT.USER_TYPE.MOBILE) {
                log.warn(`Mobile user can not login here!`);
                return res.json(utils.response(0, 'Mobile user can not login here!'));
            } else if (!user.enable) {
                log.warn(`User ${ username } is disabled to login now!`);
                return res.json(utils.response(0, `Account [${ username }] is deactivated, please contact administrator.`));
            }
            //check user lock out
            let pwdErrorTimes = user.pwdErrorTimes;
            if (pwdErrorTimes > 2) {
                return res.json(utils.response(0, `User [${ username }] is locked, please contact administrator.!`));
            }
            let userStatus = utils.CheckUserStatus(user.unLockTime, user.lastLoginTime, user.createdAt);
            if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_90) {
                return res.json(utils.response(0, `User [${ username }] is locked, last login date passed 90 days!`));
            }
            if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_180) {
                return res.json(utils.response(0, `User [${ username }] is locked, last login date passed 180 days!`));
            }
            userBase = await UserBase.findOne({where: {mvUserId: user.userId}});
        } else {
            //check password error
            let userLoginNameExist = await User.findOne({
                where: {
                    username,
                    userType: {
                        [Op.ne]: CONTENT.USER_TYPE.MOBILE
                    },
                }
            });
            if (userLoginNameExist) {
                // password is error
                let pwdErrorTimes = userLoginNameExist.pwdErrorTimes + 1;
                await User.update({pwdErrorTimes: pwdErrorTimes}, {where: { userId: userLoginNameExist.userId }});
                
                userBase = await UserBase.findOne({where: {mvUserId: userLoginNameExist.userId}});
                if (userBase && userBase.cvUserId) {
                    await _SysUser.USER.update({
                        times: pwdErrorTimes
                    }, { where: {id: userBase.cvUserId}});
                }
            } else {
                // check wait approve or has been rejected
                let userApplyExist = await UserBase.findOne({where: { loginName: username, mvUserType: { [Op.not]: null, } }});
                if (userApplyExist) {
                    if (userApplyExist.rejectBy) {
                        return res.json(utils.response(0, 'Account Registration Rejected.'));
                    } else if (userApplyExist.status == 'Pending Approval') {
                        return res.json(utils.response(0, 'Account Registration Pending Approval.'));
                    }
                }
            }

            log.warn(`User(username: ${ username }, password: ${ password } does not exist`);
            return res.json(utils.response(0, 'User Account does not exist.'));
        }

        //check user ord
        let userOrd = user.ord;
        if (userOrd && userOrd <= moment().format('YYYY-MM-DD')) {
            return res.json(utils.response(0, `Login Failed. Account [${username}] ORD Expired, please contact administrator.`));
        }

        let jwtToken = utils.generateTokenKey({ userId: user.userId })
        // Update User info
        user.online = 1;
        user.lastLoginTime = moment();
        user.pwdErrorTimes = 0;
        user.jwtToken = jwtToken;
        await user.save();

        //update system user lastLoginTime and times
        if (userBase && userBase.cvUserId) {
            await _SysUser.USER.update({
                lastLoginTime: moment(),
                times: 0
            }, { where: {id: userBase.cvUserId}});
        }

        let unit = await Unit.findByPk(user.unitId)

        // Store User into cookie
        res.cookie('token', jwtToken, { expires: utils.expiresCookieDate() });
        req.session.token = jwtToken
        res.cookie('userId', user.userId, { expires: utils.expiresCookieDate() });
        req.session.userId = user.userId;
        res.cookie('username', user.username, { expires: utils.expiresCookieDate() });
        res.cookie('fullName', user.fullName, { expires: utils.expiresCookieDate() });
        res.cookie('userType', user.userType, { expires: utils.expiresCookieDate() });
        res.cookie('email', userBase && userBase.email ? userBase.email : '', { expires: utils.expiresCookieDate() });
        res.cookie('lastLoginTime', moment().format('YYYY-MM-DD HH:mm:ss'), { expires: utils.expiresCookieDate() });
        res.cookie('needChangePassword', (user.lastChangePasswordDate ? 0 : 1), { expires: utils.expiresCookieDate() });
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
            optType: 'login', 
            remarks: utils.getClientIP(req)
        }
        await OperationRecord.create(newOptRecord);

        return res.json(utils.response(1, user));
    } catch (error) {
        log.error('(login) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.loginUseSingpass = async function (req, res) {
    let nric = req.body.nric;
    log.info('(Login)=> nric: ', nric);
    let userInfoArray = nric ? nric.split('@') : [];
    let userName = '';
    let fullName = '';
    if (userInfoArray && userInfoArray.length > 1) {
        userName = userInfoArray[0]
        fullName = userInfoArray[1]
    }
    if(!userName || !fullName) {
        return res.json(utils.response(0, 'Missing user information!'));
    }
    try {
        let user = await User.findOne({
            where: {
                username: userName,
                fullName: fullName,
                userType: {
                    [Op.ne]: CONTENT.USER_TYPE.MOBILE
                },
            }
        })

        if (user) {
            if (!user.enable) {
                log.warn(`User ${ userName } is disabled to login now!`);
                return res.json(utils.response(0, `Account [${ userName }] is deactivated, please contact administrator.`));
            }

            //check user lock out
            let pwdErrorTimes = user.pwdErrorTimes;
            if (pwdErrorTimes > 2) {
                return res.json(utils.response(0, `User [${ userName }] is locked, please contact administrator.!`));
            }
            let userStatus = utils.CheckUserStatus(user.unLockTime, user.lastLoginTime, user.createdAt);
            if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_90) {
                return res.json(utils.response(0, `User [${ userName }] is locked, last login date passed 90 days!`));
            }
            if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_180) {
                return res.json(utils.response(0, `User [${ userName }] is locked, last login date passed 180 days!`));
            }
        } else {
            log.warn(`User(username: ${ nric }}) does not exist`);
            return res.json(utils.response(0, 'User does not exist'));
        }

        let jwtToken = utils.generateTokenKey({ userId: user.userId });
        // Update User info
        user.online = 1;
        user.lastLoginTime = moment();
        user.pwdErrorTimes = 0;
        user.jwtToken = jwtToken
        await user.save();

        //update system user lastLoginTime and times
        let userBase = await UserBase.findOne({where: {mvUserId: user.userId}});
        if (userBase && userBase.cvUserId) {
            await _SysUser.USER.update({
                lastLoginTime: moment(),
                times: 0
            }, { where: {id: userBase.cvUserId}});
        }

        let unit = await Unit.findByPk(user.unitId)
        // Store User into cookie
        res.cookie('token', jwtToken, { expires: utils.expiresCookieDate() });
        req.session.token = jwtToken;
        res.cookie('userId', user.userId, { expires: utils.expiresCookieDate() });
        req.session.userId = user.userId;
        res.cookie('username', user.username, { expires: utils.expiresCookieDate() });
        res.cookie('fullName', user.fullName, { expires: utils.expiresCookieDate() });
        res.cookie('userType', user.userType, { expires: utils.expiresCookieDate() });
        res.cookie('email', userBase && userBase.email ? userBase.email : '', { expires: utils.expiresCookieDate() });
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
            businessType: 'login_singpass',
            businessId: 'laptop',
            optType: 'login', 
            remarks: utils.getClientIP(req)
        }
        await OperationRecord.create(newOptRecord);

        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error('(login) : ', error);
        return res.json(utils.response(0, error));
    }
  };

module.exports.logout = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        log.info('(Logout)=> userId: ', userId);

        let user = await User.findByPk(userId)
        if (!user) {
            log.warn(`User(userId: ${ userId } does not exist`);
            return res.json(utils.response(0, 'User does not exist'));
        }

        // Update User info
        user.online = 0;
        user.jwtToken = null;
        user.save();

        // Store User into cookie
        res.clearCookie('token');
        res.clearCookie('userId');
        res.clearCookie('username');
        res.clearCookie('appoint');
        res.clearCookie('subAppoint');
        res.clearCookie('userType');
        res.clearCookie('lastLoginTime');
        res.clearCookie('needChangePassword');
        res.clearCookie('gitsiServer');
        res.clearCookie('sysConf'); 

        req.session.token = null;
        req.session.userId = null;

        // clear cv system user's token
        let cvUser = await sequelizeObj.query(` SELECT cvUserId FROM user_base WHERE mvUserId = ? `, 
        { type: QueryTypes.SELECT, replacements: [userId] })
        if (cvUser && cvUser.length) {
            let cvUserId = cvUser[0].cvUserId;
            await sequelizeSystemObj.query(` UPDATE \`user\` SET token = NULL WHERE id = ? `, 
            { type: QueryTypes.UPDATE, replacements: [cvUserId] })
        }

        return res.json(utils.response(1, 'Success'));
    } catch (err) {
        log.error('(login) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getCurrentUser = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let userList = [];
        let baseSQL = `
            SELECT us.userId, us.username, us.fullName, us.nric, us.userType, us.driverId, us.unitId, un.unit, un.subUnit
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
        `
        if (userId) {
            userList = await sequelizeObj.query(baseSQL + ` WHERE us.userId = ? `, { type: QueryTypes.SELECT, replacements: [userId] })
        } else {
            userList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        }
        return res.json(utils.response(1, userList));
    } catch (error) {
        log.error('(getCurrentUser) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getUserList = async function (req, res) {
    try {
        let user = req.body.user;
        let userList = [];
        let baseSQL = `
            SELECT us.userId, us.role, us.username, us.fullName, us.nric, us.userType, us.driverId, us.unitId, un.unit, un.subUnit
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
        `
        if (user) {
            userList = await sequelizeObj.query(baseSQL + ` WHERE us.userId = ? `, { type: QueryTypes.SELECT, replacements: [user.userId ] })
        } else {
            userList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        }
        return res.json(utils.response(1, userList));
    } catch (error) {
        log.error('(getUserList) : ', error);
        return res.json(utils.response(0, error));
    }
};

/**
 * Every userType has only one user account!!!
 * Every userType has only one user account!!!
 * Every userType has only one user account!!!
 * @param {*} req 
 * @param {*} res 
 * @returns 
 */
module.exports.createUser = async function (req, res) {
    try {
        let user = req.body.user;
        user.username = user.nric + ((user.fullName.toString()).replace(/\s*/g,"").toUpperCase()).substr(0, 3);
       
        const checkUser = async function (user) {
            // check if username already exist
            // let userResult = await User.findOne({ where: { username: user.username, userType: { [Op.eq]: user.userType } } });
            // // while username already exist, return
            // if (userResult) {
            //     throw `Username ${ user.username } already exist!`
            // }
            // userResult = await User.findOne({ where: { nric: user.nric, userType: { [Op.eq]: user.userType } } });
            // // while nric already exist, return
            // if (userResult) {
            //     throw `Nric ${ user.nric } already exist!`
            // }

            // while unit role. need check unit
            if (user.userType === CONTENT.USER_TYPE.UNIT) {
                let unit = await Unit.findByPk(user.unitId);
                if (!unit) {
                    log.warn(`Unit ${ user.unitId } does not exist!`)
                    throw `Unit ${ user.unitId } does not exist!`
                }
            } else if(user.userType === CONTENT.USER_TYPE.CUSTOMER){
                user.unitId = user.unitId
            } else {
                // clear un-need, in case
                delete user.unitId;
            }

            /**
             * Will same username, different fullName, but same password user here.
             * Jasmin asked @2023-07-12 10:43
             * She will change password at backend  @2023-07-12 11:07
             */
            userResult = await User.findOne({ where: { username: user.username, fullName: user.fullName, userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE } } });
            // while username already exist, return
            if (userResult) {
                log.warn(`Can not create same username in HQ/UNIT/LICENSING OFFICER`)
                throw `Username ${ user.username } already exist, please change your full name !`
            }
        }
        const createUser = async function (user) {
            if((user.userType).toUpperCase() == 'MOBILE') {
                user.password = utils.generateMD5Code(user.password).toUpperCase();
            } else if((user.userType).toUpperCase() == 'CUSTOMER'){
                let group = await TaskUtils.getGroupById(user.unitId);
                if(!group) throw `The current user's unit does not exist.`
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4)) + group.groupName).toUpperCase();
            } else if ((user.userType).toUpperCase() == 'UNIT'){
                let unit = await Unit.findOne({where: { id: user.unitId }})
                if(!unit) throw `The current user's unit does not exist.`
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4)) + unit.unit).toUpperCase(); 
            } else if((user.userType).toUpperCase() == 'HQ') {
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4))+ 'BB50').toUpperCase();
            } else {
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4))).toUpperCase();
            }
            if((user.userType).toUpperCase() == 'HQ' || (user.userType).toUpperCase() == 'ADMINISTRATOR'){
                user.unitId = null
            }
            let newUser = await User.create(user, { returning: true });
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: newUser.userId,
                optType: 'New User',
                beforeData: ``,
                afterData: `${ JSON.stringify([newUser]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'create user.'
            })
            return newUser;
            
        }
        await sequelizeObj.transaction(async transaction => {
            await checkUser(user);
            await createUser(user);
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error('(createUser) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.deleteUser = async function (req, res) {
    try {
        let { user } = req.body.user;
        if (req.body.userId == user.userId) {
            throw 'Can not delete your self account!'
        }

        await sequelizeObj.transaction(async transaction => {
            const checkUser = async function (user) {
                let userResult = await User.findByPk(user.userId)
                if (!userResult) {
                    throw `UserId ${ user.userId } does not exist!`
                }
            }
            const deleteUser = async function (user) {
                await User.destroy({ where: { userId: user.userId } });
            }
            const deleteUserFromGroup = async function (user) {
                await UserGroup.destroy({ where: { userId: user.userId } })
            }
            const deleteUserFromChat = async function (user) {
                await RoomMember.destroy({ where: { roomMember: user.userId } })
            }
            const deleteUserFromNotice = async function (user) {
                await UserNotice.destroy({ where: { userId: user.userId } })
            }

            await checkUser(user);
            await deleteUser(user);
            await deleteUserFromGroup(user);
            await deleteUserFromChat(user);
            await deleteUserFromNotice(user);

            // While this user create driver/vehicle/device, then how? will not find this data any more
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error('(deleteUser) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.enableUser = async function (req, res) {
    try {
        let { enableUserId, enable } = req.body;
        await sequelizeObj.transaction(async transaction => {
            let oldUser = await User.findOne({ where: { userId: enableUserId } })
            await User.update({ enable }, { where: { userId: enableUserId } })
            let newUser = await User.findOne({ where: { userId: enableUserId } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: enableUserId,
                optType: 'Disable/Enable User',
                beforeData: `${ JSON.stringify([oldUser]) }`,
                afterData: `${ JSON.stringify([newUser]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: `disable/enable user.`
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.enableUserBase = async function (req, res) {
    let { applyId, enable } = req.body;
    let cvTransaction = null;
    let mvTransaction = null;
    try {
        let loginUser = await User.findOne({ where: { userId: req.cookies.userId } });
        if (!loginUser) throw `User does not exist.`

        let userBase = await UserBase.findByPk(applyId);
        if (!userBase) {
            return res.json(utils.response(0, 'User does not exist'));
        }
        if (userBase.status == 'Rejected') {
            return res.json(utils.response(0, 'User has rejected!'));
        }
        let systemUserId = userBase.cvUserId;
        let serverUserId = userBase.mvUserId;

        // if (systemUserId) {
        //     let systemUser = await _SysUser.USER.findByPk(systemUserId);
            
        //     if (systemUser) {
        //         if (enable == 'disable') {
        //             let sysRole = await sequelizeSystemObj.query(`
        //                 select * from role where UPPER(roleName) = 'OCC MGR'
        //             `, { type: QueryTypes.SELECT });
        //             let systemOccRoleId = null;
        //             if (sysRole && sysRole.length > 0) {
        //                 systemOccRoleId = sysRole[0].id;
        //             }
        //             // current user is occ role, check is last active occ user.
        //             if (systemOccRoleId && systemUser.role == systemOccRoleId) {
        //                 let enabledOccUserList = await sequelizeSystemObj.query(`
        //                     select u.id, u.status from user u where u.status != 'Deactivated' AND u.role=${systemOccRoleId}
        //                 `, { type: QueryTypes.SELECT });
        //                 if (enabledOccUserList && enabledOccUserList.length == 1) {
        //                     return res.json(utils.response(0, 'You should keep at least one OCC Mgr account!'));
        //                 }
        //             }
        //         }

        //         cvTransaction = await sequelizeSystemObj.transaction();

        //         let updateObj = {
        //             status: (enable == 'enable' ? 'Active' : 'Deactivated')
        //         }
        //         if (enable == 'enable') {
        //             updateObj.activeTime = moment().format('YYYY-MM-DD HH:mm:ss');
        //         }
        //         await _SysUser.USER.update(updateObj, {where: {id: systemUserId}, transaction: cvTransaction });
        //         let systemUserManagementLog = {
        //             userId: systemUserId,
        //             operatorUserBaseId: loginUserBase ? loginUserBase.id : null,
        //             activity: (enable == 'enable' ? 'Activate' : 'Deactivate'),
        //             operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
        //             triggeredBy: loginUser.fullName,
        //             remark: `Server ${enable} user:${userBase.fullName}.`
        //         }
        //         await _SysUser.UserManagementReport.create(systemUserManagementLog, { transaction: cvTransaction });
        //     }
        // }

        mvTransaction = await sequelizeObj.transaction();
        await UserBase.update({ status: (enable == 'enable' ? 'Approved' : 'Disabled') }, { where: { id: applyId }, transaction: mvTransaction })

        if (serverUserId) {
            await User.update({ enable: (enable == 'enable' ? 1 : 0) }, { where: { userId: serverUserId }, transaction: mvTransaction})
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                operatorName: loginUser.fullName,
                businessType: 'Manage User',
                businessId: applyId,
                optType: `${enable == 'enable' ? 'Activate' : 'Deactivate'}`,
                beforeData: '',
                afterData: '',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: `${enable == 'enable' ? 'Activate' : 'Deactivate'} User`,
            }, { transaction: mvTransaction });
        }

        if (cvTransaction) {
            await cvTransaction.commit();
        }
        if (mvTransaction) {
            await mvTransaction.commit();
        }
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        if (cvTransaction) {
            await cvTransaction.rollback();
        }
        if (mvTransaction) {
            await mvTransaction.rollback();
        }
        return res.json(utils.response(0, `${enable == 'enable' ? 'Enable' : 'Disable'} User fail!`,));
    }
}

module.exports.approveUserRegistApply = async function (req, res) {
    try {
        let loginUser = await User.findOne({ where: { userId: req.cookies.userId } });
        if (!loginUser) throw `User does not exist.`

        let { applyId, optType, reason } = req.body;
        let userId = req.cookies.userId;
        let userBase = await UserBase.findByPk(applyId);
        if (!userBase) {
            return res.json(utils.response(0, 'User does not exist!'));
        }

        if (!userBase.mvUserType) {
            return res.json(utils.response(0, 'User cannot approve cv user regist apply!'));
        }

        let updateObj = {};
        if (optType == 'reject') {
            if (userBase.status == 'Rejected' && userBase.rejectDate) {
                return res.json(utils.response(1, 'User is Rejected, please refresh page!'));
            }

            updateObj.status = 'Rejected';
            updateObj.rejectReason = reason;
            updateObj.rejectDate = moment().format('YYYY-MM-DD HH:mm:ss')
            updateObj.rejectBy = userId;

            await UserBase.update(updateObj, {where: {id: applyId}});
        } else {
            if (userBase.status == 'Approved' && userBase.approveDate) {
                return res.json(utils.response(1, 'User is Approved, please refresh page!'));
            }
            
            let accountUser = userBase.dataValues;
            accountUser.nric = userBase.nric;
            accountUser.approveDate = moment().format('YYYY-MM-DD HH:mm:ss')
            accountUser.approveBy = userId;

            let mvPassword = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toUpperCase();
            accountUser.nric = utils.generateAESCode(accountUser.nric).toUpperCase();
			
            let unitId = null;
            if (accountUser.mvUserType != CONTENT.USER_TYPE.HQ) {
                unitId = accountUser.mvUnitId ? accountUser.mvUnitId : accountUser.mvGroupId;
            } else {
                //update unit userId
            }
            let mvUser = await User.create({
                username: accountUser.loginName,
                fullName: accountUser.fullName,
                nric: accountUser.nric,
                ord: userBase.ord,
                unitId: unitId,
                password: mvPassword,
                userType: accountUser.mvUserType,
                role: accountUser.mvRoleName,
                contactNumber: accountUser.contactNumber,
                hq: accountUser.hq
            });
            let mvUserId = mvUser.userId;
            let updateObj = {mvUserId: mvUserId};
            updateObj.approveDate = moment().format('YYYY-MM-DD HH:mm:ss')
            updateObj.approveBy = userId;
            // just mv user or cv has approved
            //if (!userBase.cvRole || userBase.cvUserId) {
                updateObj.status = 'Approved';
            //}
            await UserBase.update(updateObj, {where: {id: applyId}});
        }
        await OperationRecord.create({
            operatorId: req.cookies.userId,
            operatorName: loginUser.fullName,
            businessType: 'Manage User',
            businessId: applyId,
            optType: optType == 'reject' ? 'Account Rejection' : 'Account Approval',
            beforeData: `${userBase.status}`,
            afterData: optType == 'reject' ? 'Rejected' : 'Approved',
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: `Approval user base.`
        });
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

/**
 * Every userType has only one user account!!!
 * Every userType has only one user account!!!
 * Every userType has only one user account!!!
 * @param {*} req 
 * @param {*} res 
 * @returns 
 */
module.exports.updateUser = async function (req, res) {
    try {
        let user = req.body.user;
        if (user.fullName) {
            user.username = user.nric + ((user.fullName.toString()).replace(/\s*/g,"").toUpperCase()).substr(0, 3);
        }

        const checkUser = async function (user) {
            if (user.username) {
                // check if username already exist
                // let userResult = await User.findOne({ where: { username: user.username, userType: { [Op.eq]: user.userType } } });
                // // while username already exist, return
                // // if(user.userId == userResult.userId && user.userType == userResult.userType){
                //     if (userResult) {
                //         if(user.userId != userResult.userId){
                //             throw `Username ${ user.username } already exist!`
                //         } else {
                //             log.info(`Update user`)
                //         }
                //     }
                // }


                // if (user.nric) {
                //     userResult = await User.findOne({ where: { nric: user.nric, userType: { [Op.eq]: user.userType } } });
                //     // while nric already exist, return
                //     if (userResult && user.userId != userResult.userId) {
                //         throw `Nric ${ user.nric } already exist!`
                //     }
                // }

                // can not exist same username in HQ and UNIT and LICENSING OFFICER
                let userResult = await User.findOne({ where: { username: user.username, 
                    userType: { [Op.ne]: CONTENT.USER_TYPE.MOBILE } , userId: {[Op.ne]: user.userId}} });
                // while username already exist, return
                if (userResult) {
                    log.warn(`Can not create same username in HQ/UNIT/LICENSING OFFICER/CUSTOMER`)
                    throw `Username ${ user.username } already exist !`
                }
            }
        }
        const updateUser = async function (user) {
            let oldUser = await User.findOne({where: { userId: user.userId }})
            if((user.userType).toUpperCase() == 'MOBILE') {
                user.password = utils.generateMD5Code(user.password).toUpperCase();
            } else if((user.userType).toUpperCase() == 'CUSTOMER'){
                let group = await TaskUtils.getGroupById(user.unitId);
                if(!group) throw `The current user's unit does not exist.`
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4)) + group.groupName).toUpperCase();
            } else if ((user.userType).toUpperCase() == 'UNIT'){
                let unit = await Unit.findOne({where: { id: user.unitId }})
                if(!unit) throw `The current user's unit does not exist.`
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4)) + unit.unit).toUpperCase();
            } else if((user.userType).toUpperCase() == 'HQ') {
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4))+ 'BB50').toUpperCase();
            } else {
                user.password = utils.generateMD5Code((user.nric.substr(((user.nric).length)-4, 4))).toUpperCase();
            }
            if((user.userType).toUpperCase() == 'HQ' || (user.userType).toUpperCase() == 'LICENSING OFFICER'  || (user.userType).toUpperCase() == 'ADMINISTRATOR'){
                user.unitId = null
            }
            await User.update(user, { where: { userId: user.userId } });
            let newUser = await User.findOne({ where: { userId: user.userId } });
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: newUser.userId,
                optType: 'Edit User',
                beforeData: `${ JSON.stringify([oldUser]) }`,
                afterData: `${ JSON.stringify([newUser]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit user.'
            })
        }

        let currentUser = await User.findOne({ where: { userId: user.userId } });
        if (currentUser && currentUser.userType === CONTENT.USER_TYPE.MOBILE) {
            if (user.unitId) {
                await Driver.update({ unitId: user.unitId }, { where: { driverId: currentUser.driverId } })
                await DriverPosition.update({ unitId: user.unitId }, { where: { driverId: currentUser.driverId } })
            }
        }
        if (!user.userType) {
            user.userType = currentUser.userType;
        }
        await sequelizeObj.transaction(async transaction => {
            await checkUser(user);
            await updateUser(user);
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error('(updateUser) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getRoleVocation = async function (req, res) {
    try {
        let user = await User.findOne({ where: { userId: req.cookies.userId } });
        if (!user) throw `User does not exist.`
        if(user.userType.toUpperCase() == 'CUSTOMER') {
            return res.json(utils.response(1, {
                "DV": [ "-" ],
                "LOA": [ "-" ]
            }));
        }
        return res.json(utils.response(1, roleVocation));
    } catch (error) {
        log.error('(getRoleVocation) : ', error);
        return res.json(utils.response(0, error));
    }
}

module.exports.getUnitList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let user = await User.findByPk(userId);
        if (!user) throw `User does not exist.`

        let pageList = await getUserPageList(userId, 'Unit', 'View Unit')
        let operation = pageList.map(item => item.action).join(',')

        let unitList = [];
        if (user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            unitList = await Unit.findAll();
        } else if (user.userType === CONTENT.USER_TYPE.HQ) {
            if (user.hq) {
                unitList = await sequelizeObj.query(`
                    SELECT * 
                    FROM unit 
                    WHERE FIND_IN_SET(?, hq)
                `, {
                    type: QueryTypes.SELECT,
                    replacements: [ `${ user.hq }` ]
                })
            }
        } else if (user.userType === CONTENT.USER_TYPE.UNIT || user.userType === CONTENT.USER_TYPE.LICENSING_OFFICER) {
            let unit = await Unit.findByPk(user.unitId)
            if (unit) {
                if (unit.subUnit) {
                    unitList = [ unit ];
                } else {
                    unitList = await Unit.findAll({ where: { unit: unit.unit } })
                }
            }
        }

        for (let unit of unitList) {
            unit.operation = operation
        }

        return res.json(utils.response(1, unitList));
    } catch (error) {
        log.error('(getUnitList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getUnit = async function (req, res) {
    try {
        let unit = await Unit.findByPk(req.body.id);
        return res.json(utils.response(1, unit));
    } catch (error) {
        log.error('(getUnit) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.createUnit = async function (req, res) {
    try {
        let unit = req.body.unit;
        let result = await Unit.findAll({ where: { unit: unit.unit, subUnit: unit.subUnit } })
        if (result.length) {
            throw `Unit already exist.`
        }
        await sequelizeObj.transaction(async transaction => {
            await Unit.create({ unit: unit.unit, subUnit: unit.subUnit })
            let newUnit = await Unit.findOne({ where: { unit: unit.unit, subUnit: unit.subUnit } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: newUnit.id,
                optType: 'New Unit',
                beforeData: ``,
                afterData: `${ JSON.stringify([newUnit]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'create unit.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error('(getUnitList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.updateUnit = async function (req, res) {
    try {
        let unit = req.body.unit;
        let result = await Unit.findAll({ where: { unit: unit.unit, subUnit: unit.subUnit } })
        if (result.length) {
            throw `Unit already exist.`
        }
        await sequelizeObj.transaction(async transaction => {
            let oldUnit = await Unit.findOne({ where: { id: unit.id } })
            await Unit.update({ unit: unit.unit, subUnit: unit.subUnit }, { where: { id: unit.id } })
            let newUnit = await Unit.findOne({ where: { id: unit.id } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: newUnit.id,
                optType: 'Edit Unit',
                beforeData: `${ JSON.stringify([oldUnit]) }`,
                afterData: `${ JSON.stringify([newUnit]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit unit.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error('(getUnitList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.deleteUnit = async function (req, res) {
    try {
        let unitId = req.body.unitId;
        let result = await User.findAll({ where: { unitId: unitId } })
        if (result.length) {
            throw `Unit still in use.`
        }
        await sequelizeObj.transaction(async transaction => {
            let oldUnit = await Unit.findOne({ where: { id: unitId } })
            await Unit.destroy({ where: { id: unitId } })
            let newUnit = await Unit.findOne({ where: { id: unitId } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Manage User',
                businessId: unitId,
                optType: 'Delete Unit',
                beforeData: `${ JSON.stringify([oldUnit]) }`,
                afterData: `${ JSON.stringify([newUnit ? newUnit : '']) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'delete unit.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error('(getUnitList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getUserTypeList = async function (req, res) {
    try {
        let { checkRole } = req.body;
        let userTypeList = Object.values(CONTENT.USER_TYPE)
        if (checkRole) {
            let user = await userService.UserUtils.getUserDetailInfo(req.body.userId)

            if (user.userType == CONTENT.USER_TYPE.ADMINISTRATOR) {

            } else if (user.userType == CONTENT.USER_TYPE.HQ) {
                userTypeList = userTypeList.filter(userType => ![ CONTENT.USER_TYPE.ADMINISTRATOR ].includes(userType))
            } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
                userTypeList = userTypeList.filter(userType => ![ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ ].includes(userType))
            } else if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                userTypeList = [ CONTENT.USER_TYPE.CUSTOMER ]
            } else if (user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
                userTypeList = [ CONTENT.USER_TYPE.LICENSING_OFFICER ]
            }
        }

        userTypeList = userTypeList.filter(userType => ![ CONTENT.USER_TYPE.MOBILE ].includes(userType))
        return res.json(utils.response(1, userTypeList));
    } catch (error) {
        log.error('(getUserTypeList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getLaptopUserList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let fullName = req.body.fullName;
        let user = await getUserDetailInfo(userId);
        if (!user) {
            throw `UserId ${ userId } does not exist.`
        }

        let pageList = await getUserPageList(userId, 'User Management', 'View User')
        let operation = pageList.map(item => item.action).join(',')

        let userList = []
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            let selectOption = { userType: [CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.UNIT, CONTENT.USER_TYPE.LICENSING_OFFICER, CONTENT.USER_TYPE.CUSTOMER] }
            if (fullName) {
                selectOption.fullName = { [Op.substring]: fullName }
            }
            userList = await User.findAll({ where: selectOption })
        } else if(user.userType === CONTENT.USER_TYPE.CUSTOMER){
            let option = [];
            if (user.unitId) option.push({ unitId: user.unitId })
            if (option.length) {
                let selectOption = { [Op.or]: option, userType: [CONTENT.USER_TYPE.CUSTOMER] }
                if (fullName) {
                    selectOption.fullName = { [Op.substring]: fullName }
                }
                userList = await User.findAll({ where: selectOption })
            }
        } else {
            let unitIdList = await unitService.getUnitPermissionIdList(user);
            // let userIdList = await groupService.getGroupUserIdListByUser(user);
            let option = [];
            if (unitIdList.length) option.push({ unitId: unitIdList })
            // if (userIdList.length) option.push({ userId: userIdList })
            if (option.length) {
                let selectOption = { [Op.or]: option, userType: [CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.UNIT, CONTENT.USER_TYPE.LICENSING_OFFICER] }
                if (fullName) {
                    selectOption.fullName = { [Op.substring]: fullName }
                }
                userList = await User.findAll({ where: selectOption })
            }
        }

        for (let user of userList) {
            user.operation = operation
        }
        return res.json(utils.response(1, userList));
    } catch (error) {
        log.error('(getLaptopUserList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getMobileUserList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let username = req.body.username;
        let user = await getUserDetailInfo(userId);
        if (!user) {
            throw `UserId ${ userId } does not exist.`
        }

        let pageList = await getUserPageList(userId, 'User Management', 'View Mobile User')
        let operation = pageList.map(item => item.action).join(',')

        let userList = []
        if (user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            let option = { enable: 1 }
            option.userType = [ CONTENT.USER_TYPE.MOBILE ];
            if (username) {
                option.username = { [Op.substring]: username };
            }
            userList = await User.findAll({ where: option })
        } else {
            let unitIdList = await unitService.getUnitPermissionIdList(user);

            if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                let option = { groupId: user.unitId, enable: 1 }
                if (username) option.username = username
                userList = await getMobileUserList(option)
            } else {
                let option = { unitId: unitIdList, enable: 1 }
                if (username) option.username = username
                userList = await getMobileUserList(option)
            }
        }

        for (let user of userList) {
            user.operation = operation
        }
        return res.json(utils.response(1, userList));
    } catch (err) {
        log.error('(getMobileUserList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getCVMVUserList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let { tabPage, searchCondition, cvRoleId, mvUserType, pageNum, pageLength, orderField, orderType } = req.body;
        let loginUser = await getUserDetailInfo(userId);
        if (!loginUser) {
            throw `UserId ${ userId } does not exist.`
        }

        let pageList = await getUserPageList(userId, 'User Management', 'View User')
        let pageList2 = await userService.getUserPageList(userId, 'View Full NRIC')
        if(pageList2.length > 0) {
            pageList2[0].action = 'View Full NRIC'
            pageList = pageList.concat(pageList2);
        }
        let operation = pageList.map(item => item.action).join(',')

        let baseSql = `
            select * from (
                SELECT
                    ub.id, ub.cvUserId, ub.mvUserId,
                    ub.fullName, ub.loginName, ub.nric, ub.ord,
                    ub.cvRole, ub.cvGroupId, ub.cvGroupName,
                    ub.mvUserType, ub.mvUnitId, 
                    IF(ub.mvUserType = 'HQ', NULL, un.unit) AS mvHub,
                    IF(ub.mvUserType = 'HQ', NULL, un.subUnit) AS mvNode,
                    ub.mvGroupId, ub.mvGroupName, ub.mvRoleName,
                    ub.dataFrom, ub.status, ub.createdAt, ub.creator, uu1.fullName as createrName,
                    ub.approveDate, ub.approveBy, uu2.fullName as approvedUserName,
                    ub.rejectBy, uu.fullName as rejectName, ub.rejectDate, ub.rejectReason
                FROM user_base ub
                LEFT JOIN unit un ON ub.mvUnitId = un.id
                LEFT JOIN user uu ON uu.userId = ub.rejectBy
                LEFT JOIN user uu1 ON uu1.userId = ub.creator
                LEFT JOIN user uu2 ON uu2.userId = ub.approveBy
                where ub.mvUserType is not null
            ) ub where 1=1 
        `;
        
        let baseCountSql = `
            SELECT count(*) as count from (
                SELECT
                    ub.*,
                    IF(ub.mvUserType = 'HQ', NULL, un.unit) AS mvHub,
                    IF(ub.mvUserType = 'HQ', NULL, un.subUnit) AS mvNode
                FROM user_base ub
                LEFT JOIN unit un ON ub.mvUnitId = un.id
                where ub.mvUserType is not null
            ) ub where 1=1 
        `;
        
        let replacements = []
        let limitCondition = [];
        if (loginUser.userType == CONTENT.USER_TYPE.CUSTOMER) {
            limitCondition.push(` ub.mvGroupId='${loginUser.unitId}' `);
        } else if (loginUser.userType == CONTENT.USER_TYPE.UNIT) {
            let userUnitId = loginUser.unitId;
            let userUnit = await Unit.findByPk(userUnitId);
            if (userUnit) {
                if (userUnit.unit) {
                    limitCondition.push(` ub.mvHub=? `)
                    replacements.push(userUnit.unit)
                }
                if (userUnit.subUnit) {
                    limitCondition.push(` ub.mvNode=? `)
                    replacements.push(userUnit.subUnit)
                }
            }
        } else if (loginUser.userType == CONTENT.USER_TYPE.HQ) {
            //close hq
            // let userBase = await UserBase.findOne({where: {mvUserId: userId}});
            // let hqUserType = '';
            // if (userBase) {
            //     hqUserType = userBase.hq;
            // }
            // let hqUserAllUnitIds = [];
            // hqUserAllUnitIds = await unitService.getUnitPermissionIdList(loginUser);
            // if (hqUserType) {
            //     if (hqUserAllUnitIds.length > 0) {
            //         limitCondition.push(` (ub.mvUserId='${userId}' or ub.mvUnitId in(${hqUserAllUnitIds})) `);
            //     } else {
            //         limitCondition.push(` ub.mvUserId='${userId}' `);
            //     }
            // } else {
            //     return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0, pendingApprovalNum });
            // }
        }
        
        if (cvRoleId) {
            limitCondition.push(` ub.cvRole = ? `)
            replacements.push(cvRoleId)
        }
        if (mvUserType) {
            limitCondition.push(` ub.mvUserType = ? `)
            replacements.push(mvUserType)
        }
        if (searchCondition) {
            limitCondition.push(` (
                ub.fullName LIKE ?
                OR ub.mvHub like ?
                OR ub.mvNode LIKE ?
                OR ub.cvGroupName LIKE ? 
                OR ub.mvGroupName LIKE ?
                OR ub.mvRoleName LIKE ?
            ) `)
            replacements.push('%'+ searchCondition +'%')
            replacements.push('%'+ searchCondition +'%')
            replacements.push('%'+ searchCondition +'%')
            replacements.push('%'+ searchCondition +'%')
            replacements.push('%'+ searchCondition +'%')
            replacements.push('%'+ searchCondition +'%')
        }

        let pendingApproveNumSql = baseCountSql;
        if (limitCondition.length) {
            pendingApproveNumSql += ' AND ' +  limitCondition.join(' AND ');
        }

        if (tabPage == 'Approved') {
            limitCondition.push(` ub.mvUserId is not null `)
            limitCondition.push(` ub.status != 'Disabled' `)
        } else if (tabPage == 'Pending Approval') {
            limitCondition.push(` ub.mvUserId is null `)
            limitCondition.push(` ub.rejectBy is null `)
        } else if (tabPage == 'Rejected') {
            limitCondition.push(` ub.rejectBy is not null `)
        } else {
            limitCondition.push(` ub.status=? `)
            replacements.push(tabPage)
        }
        if (limitCondition.length) {
            baseSql += ' AND ' + limitCondition.join(' AND ');
            baseCountSql += ' AND ' +  limitCondition.join(' AND ');
        }

        let totalList = await sequelizeObj.query(baseCountSql, { type: QueryTypes.SELECT, replacements: replacements });
        let pendingApprovalNum = 0;
        if (tabPage == 'Pending Approval') {
            pendingApprovalNum = totalList[0].count;
        } else {
            pendingApproveNumSql += ` and ub.mvUserId is null and ub.rejectBy is null `;
            let pendingApproveTotalList = await sequelizeObj.query(pendingApproveNumSql, { type: QueryTypes.SELECT, replacements: replacements });
            pendingApprovalNum = pendingApproveTotalList[0].count;
        }
        
        if (orderField) {
            if (orderField == 'unit') {
                baseSql += ` order by ub.mvHub ` + orderType + ', ' + 'ub.mvNode' + orderType;
            } else if (orderField == 'createdAt') {
                baseSql += ` order by ub.createdAt` + orderType;
            } else if (orderField == 'ord') {
                baseSql += ` order by ub.createdAt ` + orderType;
            } else {
                baseSql += ` order by ub.fullName ` + orderType;
            }
        } else {
            if (tabPage == 'Pending Approval' || tabPage == 'Rejected') {
                baseSql += ` ORDER BY ub.createdAt DESC `
            } else {
                baseSql += ` ORDER BY ub.approveDate DESC `
            }
        }
        
        baseSql += ` limit ?, ?`;
        replacements.push(...[Number(pageNum), Number(pageLength)])
        let userList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements: replacements });

        let sysRoleList = await sequelizeSystemObj.query(`
           select * from role 
        `, { type: QueryTypes.SELECT });

        for (let user of userList) {
            user.operation = operation
            let userRoleObj = sysRoleList.find(item => item.id == user.cvRole);
            if (userRoleObj) {
                user.cvRoleName = userRoleObj.roleName;
            }

            user.canApprove = 1;
            // while role is "God Mode" / "CO" / "B2/S3" only HQ can approve
            if (JUST_HQ_APPROVE_ROLE && JUST_HQ_APPROVE_ROLE.includes(user.mvRoleName) && loginUser.userType !== CONTENT.USER_TYPE.HQ) {
                user.canApprove = -1;
            }
            if (operation && operation.indexOf('Approval Status') == -1) {
                user.canApprove = -1;
            }

            if(user.nric) {
                user.nric = utils.decodeAESCode(user.nric);
            }
            //set lastLoginTime
            let cvUserId = user.cvUserId;
            let mvUserId = user.mvUserId;
            let lastLoginAt = '';
            let lastLoginTime = '';
            let cvLastLoginTime = '';
            let mvLastLoginTime = '';
            let cvActiveTime = null;
            let mvActiveTime = null;
            let cvCreatedAt = null;
            let mvCreatedAt = null;
            let cvPwdErrorTimes = 0;
            let mvPwdErrorTimes = 0;
            if (cvUserId) {
                let cvUser = await _SysUser.USER.findByPk(cvUserId);
                if (cvUser) {
                    cvLastLoginTime = cvUser.lastLoginTime;
                    cvActiveTime = cvUser.activeTime;
                    cvPwdErrorTimes = cvUser.times;
                    cvCreatedAt = cvUser.createdAt;
                }
            }
            if (mvUserId) {
                let mvUser = await User.findByPk(mvUserId);
                if (mvUser) {
                    mvLastLoginTime = mvUser.lastLoginTime;
                    mvActiveTime = mvUser.unLockTime;
                    mvPwdErrorTimes = mvUser.pwdErrorTimes;
                    mvCreatedAt = mvUser.createdAt;
                }
            }
            if (cvLastLoginTime && mvLastLoginTime) {
                if (moment(cvLastLoginTime).isBefore(moment(mvLastLoginTime))) {
                    lastLoginAt = 'MV';
                    lastLoginTime = mvLastLoginTime;
                } else {
                    lastLoginAt = 'CV';
                    lastLoginTime = cvLastLoginTime;
                }
            } else {
                if (cvLastLoginTime) {
                    lastLoginAt = 'CV';
                    lastLoginTime = cvLastLoginTime;
                }
                if (mvLastLoginTime) {
                    lastLoginAt = 'MV';
                    lastLoginTime = mvLastLoginTime;
                }
            }
            user.lastLoginAt = lastLoginAt;
            user.lastLoginTime = lastLoginTime;

            if (tabPage == 'Approved') {
                //calc user status
                if (cvPwdErrorTimes >= 3 || mvPwdErrorTimes >= 3) {
                    user.status = 'Lock Out';
                    user.lockOutDesc = 'Password input error exceeds 3 times.';
                    continue;
                } else {
                    if (cvCreatedAt) {
                        let userStatus = utils.CheckUserStatus(cvActiveTime, cvLastLoginTime, cvCreatedAt);
                        if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_90) {
                            user.status = 'Lock Out';
                            user.lockOutDesc = 'Last login date passed 90 days';
                            continue;
                        } else if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_180) {
                            user.status = 'Lock Out';
                            user.lockOutDesc = 'Last login date passed 90 days';
                            continue;
                        }
                    }
                    if (mvCreatedAt) {
                        let userStatus = utils.CheckUserStatus(mvActiveTime, mvLastLoginTime, mvCreatedAt);
                        if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_90) {
                            user.status = 'Lock Out';
                            user.lockOutDesc = 'Last login date passed 90 days';
                            continue;
                        } else if (userStatus == CONTENT.USER_STATUS.LOCK_OUT_180) {
                            user.status = 'Lock Out';
                            user.lockOutDesc = 'Last login date passed 90 days';
                            continue;
                        }
                    }
                }
            }
        }
        return res.json({ respMessage: userList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count, pendingApprovalNum });
    } catch (err) {
        log.error('(getCVMVUserList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getMobileUser = async function (req, res) {
    try {
        let user = req.body.user;
        user.userType = CONTENT.USER_TYPE.MOBILE;
        let result = await User.findAll({ where: user })
        if (!result.length) {
            throw `User ${ user } does not exist.`
        }
        for (let __user of result) {
            let driver = await Driver.findByPk(__user.driverId)
            if (driver.unitId) {
                let unit = await Unit.findByPk(driver.unitId);
                if (unit) {
                    result.unit = unit.unit;
                    result.subUnit = unit.subUnit;
                }
            }
        }

        return res.json(utils.response(1, result));
    } catch (err) {
        log.error('(getMobileUserList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteMobileUser = async function (req, res) {
    try {
        const checkDriver = async function (user) {
            let vehicleRelationList = await VehicleRelation.findAll({ where: { driverId: user.driverId } })
            let vehicleRelationIdList = vehicleRelationList.map(vehicleRelation => vehicleRelation.id);
            let driverTaskList = await DriverTask.findAll({ where: { vehicleRelationId: vehicleRelationIdList } })
            if (driverTaskList.length) {
                throw ` Driver ${ user.username } can not be deleted now. `
            }
        }
        await sequelizeObj.transaction(async transaction => {
            let userId = req.body.deleteUserId;
            let user = await User.findOne({ where: { userId, userType: CONTENT.USER_TYPE.MOBILE } })
            await checkDriver(user);
            await user.destroy();
            await Driver.destroy({ where: { driverId: user.driverId } })
            await UserGroup.destroy({ where: { userId: user.userId } })
            await VehicleRelation.destroy({ where: { driverId: user.driverId } })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error('(getMobileUserList) : ', error);
        return res.json(utils.response(0, error));
    }
};

const getUserDetailInfo = async function (userId) {
    try {
        let userList = await sequelizeObj.query(`
            SELECT us.userId, us.username, us.userType, us.driverId, us.unitId, un.unit, un.subUnit, us.role, us.hq
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
            WHERE us.userId = ? LIMIT 1
        `, { replacements: [userId], type: QueryTypes.SELECT })
        if (!userList.length) {
            return null;
        } else {
            return userList[0];
        }
    } catch (error) {
        log.error('(getUserDetailInfo) : ', error);
        return null;
    }
};
module.exports.getUserDetailInfo = getUserDetailInfo;


// *****************************************************************

const getUserPageList = async function (userId, module, page) {
    try {
        const getUserManageLink = async function () {
            let result = await ModulePage.findAll({ where: { id: [ 200, 201, 203, 204 ] } });
            return result
        }
        const getRoleManageLink = async function () {
            let result = await ModulePage.findAll({ where: { id: [ 310, 320, 330, 340 ] } });
            return result
        }

        let user = await User.findByPk(userId);
        if (!user) {
            throw `UserID ${ userId } does not exist.`
        }

        let sql = `
                SELECT mp.module, mp.page, mp.action, mp.link FROM \`role\` r
                LEFT JOIN module_page mp ON FIND_IN_SET(mp.id, r.pageList)
                WHERE r.roleName = ?
            `
        let replacements = [ user.role ]

        if (module) {
            sql += ` AND mp.module = ? `
            replacements.push(module)
        }
        if (page) {
            sql += ` AND mp.page = ? `
            replacements.push(page)
        }

        let pageList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })

        if (!module) {
            // UserType => ADMINISTRATOR default has role & user permission
            if (user.userType == CONTENT.USER_TYPE.ADMINISTRATOR) {
                let userPageList = await getUserManageLink()
                let rolePageList = await getRoleManageLink()
                pageList = pageList.concat(userPageList, rolePageList)
            }
        }
        return pageList
    } catch (error) {
        log.error(`(getUserPageList): `, error)
        return []
    }
}
module.exports.getUserPageList = getUserPageList

module.exports.getSystemUrl = async function (req, res) {
    try {
        return res.json(utils.response(1, conf.systemServer));
    } catch (err) {
        log.error('(getSystemUrl) : ', err);
        return res.json(utils.response(0, err));
    }
}

module.exports.getSystemRole = async function (req, res) {
    try {
        let sysRole = await sequelizeSystemObj.query(`
           select * from role 
        `, { type: QueryTypes.SELECT })
        return res.json(utils.response(1, sysRole));
    } catch (err) {
        log.error('(getSystemRole) : ', err);
        return res.json(utils.response(0, err));
    }
}

module.exports.getAccountUserData = async function (req, res) {
    try {
        let userId = req.body.userStatus == true ? req.cookies ? req.cookies.userId : null : null;
        let loginUser = null;
        if(userId) {
            loginUser = await User.findOne({ where: { userId: userId } })
        }
        let sql = ` select id, groupName from \`group\` `
        let replacementsGroup = []
        if(loginUser){
            if(loginUser.userType.toUpperCase() == 'CUSTOMER'){
                sql += ` where id = ?`
                replacementsGroup.push(loginUser.unitId)
            }
        }
        let groupList = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT, replacements: replacementsGroup });
        let unitList = await Unit.findAll()
        if(loginUser){
            if(loginUser.userType.toUpperCase() == 'CUSTOMER'){
                unitList = []
            } else if(loginUser.unitId) {
                let unitList2 = await Unit.findOne({ where: { id: loginUser.unitId } })
                if(unitList2){
                    if(!unitList2.subUnit) {
                        unitList = await Unit.findAll({ where: { unit: unitList2.unit } })
                    } else {
                        unitList = [unitList2]
                    }
                }
            } else if(loginUser.hq){
                let unitTypeList = await sequelizeObj.query(`
                    SELECT * FROM unit WHERE hq is not null
                    and FIND_IN_SET(?, hq)
                    group by id order by unit, subUnit
                `, { type: QueryTypes.SELECT, replacements: [loginUser.hq] })
                unitList = unitTypeList;
            }
        }
        let userTypeList = Object.values(CONTENT.USER_TYPE)
        userTypeList = userTypeList.filter(item => item != 'LICENSING OFFICER')
        if(loginUser) {
            if(loginUser.userType.toUpperCase() == 'HQ') {
                // userTypeList = ['HQ', 'UNIT', 'LICENSING OFFICER', 'CUSTOMER']
                userTypeList = userTypeList.filter(item => item != 'ADMINISTRATOR')
            } else {
                if(loginUser.userType.toUpperCase() != 'ADMINISTRATOR'){
                    userTypeList = [loginUser.userType]
                }
            }
        }
        let roleList = await sequelizeObj.query(`
        SELECT r.*, u.fullName as creator, u2.fullName as updater FROM \`role\` r
        left join user u on u.userId = r.creator
        left join user u2 on u2.userId = r.updater
        `, { type: QueryTypes.SELECT })
        let sysServiceProviderList = await sequelizeSystemObj.query(`
        select * from service_provider
        `, { type: QueryTypes.SELECT })
        let data = {
            groupList: groupList,
            unitList: unitList,
            userTypeList: userTypeList,
            roleList: roleList,
            sysServiceProviderList: sysServiceProviderList
        }
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.GetServiceTypeBySelectedGroup = async function (req, res) {
    try {
        let groupId = req.body.selectedGroupId
        let type = await sequelizeSystemObj.query(` 
        select serviceType from \`group\` where id = ?
        `, { type: QueryTypes.SELECT, replacements: [groupId] });
        type = type.map(item => item.serviceType)
        let result = await sequelizeSystemObj.query(` 
            select * from service_type where id in(?) `, { type: QueryTypes.SELECT, replacements: [type] });
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.registerAccountUser = async function (req, res) {
    let serverAffair = null;
    let systemAffair = null;
    try {
        let accountUser = req.body.accountUser;
        // keep "UNIT" in user
        if (['hub', 'node'].includes(accountUser.mvUserType.toLowerCase())) {
            accountUser.mvUserType = CONTENT.USER_TYPE.UNIT
        }
        let newNric = ((accountUser.nric).toString()).substr(0, 1)+((accountUser.nric).toString()).substr(((accountUser.nric).toString()).length-4, 4);
        let password = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toUpperCase();
        accountUser.loginName = newNric + ((accountUser.fullName.toString()).replace(/\s*/g,"").toUpperCase()).substr(0, 3);
        accountUser.password = password
        let userBaseStatus = await TaskUtils.getUserByData(null, accountUser.nric, accountUser.contactNumber, accountUser.loginName, accountUser.fullName)
        if(userBaseStatus) throw userBaseStatus
        if(accountUser.dataFrom.toUpperCase() == 'SERVER-USER'){
            accountUser.creator = req.cookies.userId
            accountUser.approveDate = moment().format('YYYY-MM-DD HH:mm:ss')
            accountUser.approveBy = req.cookies.userId
            // prove the existence
            let userBases = await UserBase.findOne({ where: { loginName: accountUser.loginName, password: accountUser.password } })
            // let errData = await TaskUtils.initCVAndMVUser(accountUser, 'create')
            let errData = null;
            if(userBases){
                if(userBases.cvRejectDate || userBases.rejectDate){
                    errData = await TaskUtils.initCVAndMVUser(accountUser, 'homeregistration', userBases.id)
                }
            } else {
                errData = await TaskUtils.initCVAndMVUser(accountUser, 'create')
            }
            if(errData) throw ` Creation failure.`
            return res.json(utils.response(1, true));
        } else { 
            serverAffair = await sequelizeObj.transaction();
            systemAffair = await sequelizeSystemObj.transaction();
            accountUser.password = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toUpperCase();
            accountUser.nric = utils.generateAESCode(accountUser.nric).toUpperCase();
            let userBases = await UserBase.findOne({ where: { loginName: accountUser.loginName, password: accountUser.password } })
            let userBase = null;
            if(userBases) {
                //Reapply for a cv user
                if(userBases.cvRejectDate && accountUser.cvRole) {
                    await UserBase.update({ cvRejectDate: null, cvRejectBy: null, cvRejectReason: null }, { where: { id: userBases.id }, transaction: serverAffair });
                }
                 //Reapply for a mv user
                if(userBases.rejectDate && accountUser.mvUserType) {
                    await UserBase.update({ status: 'Pending Approval', rejectDate: null, rejectBy: null, rejectReason: null }, { where: { id: userBases.id }, transaction: serverAffair });
                }
                if(userBases.cvRejectDate || userBases.rejectDate){
                    await UserBase.update(accountUser, { where: { id: userBases.id }, transaction: serverAffair });
                }
                userBase = await UserBase.findOne({ where: { id: userBases.id } })
            } else {
                userBase = await UserBase.create(accountUser, { transaction: serverAffair })
            }
            if(accountUser.mvUserType){
                await OperationRecord.create({
                    id: null,
                    operatorId: 0,
                    businessType: 'Manage User',
                    operatorName: accountUser.fullName,
                    businessId: userBase.id,
                    optType: 'Account Register',
                    afterData: `${ JSON.stringify(userBase) }`,
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks:  `Account Register ${ accountUser.dataFrom }`
                }, { transaction: serverAffair })
            }

            if(accountUser.cvRole){
                await _SysUser.UserManagementReport.create({
                    operatorUserBaseId: userBase.id,
                    triggeredBy: accountUser.fullName,
                    activity: 'Account Register',
                    afterData: `${ JSON.stringify(userBase) }`,
                    operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remark:  `Account Register ${ accountUser.dataFrom }`
                }, { transaction: systemAffair })
            }
            await systemAffair.commit();
            await serverAffair.commit();

            return res.json(utils.response(1, true));
        }
    } catch (error) {
        if(systemAffair) await systemAffair.rollback();
        if(serverAffair) await serverAffair.rollback();
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserBaseById = async function (req, res) {
    try {
        let userBaseId = req.body.userBaseId;
        let unitHubNode = null;
        let userBase = await UserBase.findOne({ where: { id: userBaseId } })
        if(userBase.mvUnitId){
            let unit = await Unit.findOne({ where: { id: userBase.mvUnitId } })
            if(unit){
                unitHubNode = {
                    hub: unit.unit,
                    node: unit.subUnit ?? null
                }
            }
        }
        return res.json(utils.response(1, { data: userBase, mvUnit: unitHubNode }));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.editAccountUser = async function (req, res) {
    try {
        let accountUser = req.body.accountUser;
        let userBaseId = req.body.userBaseId;
        let dataUserType = req.body.dataUserType;
        let oldUserBase = await UserBase.findOne({ where: { id: userBaseId } })
        let optionType = 'homeregistration'
        if(!dataUserType) {
            optionType = 'edit'
            accountUser.approveDate = moment().format('YYYY-MM-DD HH:mm:ss')
            accountUser.approveBy = req.cookies.userId
        }
        
        if(optionType != 'homeregistration') {
            if(oldUserBase.cvRejectBy){
                if(accountUser.cvRole != oldUserBase.cvRole || accountUser.cvGroupId != oldUserBase.cvGroupId 
                    || accountUser.cvServiceProviderId != oldUserBase.cvServiceProviderId || oldUserBase.cvServiceTypeId != accountUser.cvServiceTypeId){
                        return res.json(utils.response(0, ` It has been rejected. CV information cannot be edited!`));
                }
            }
            if(oldUserBase.rejectBy){
                if(accountUser.mvUserType != oldUserBase.mvUserType || accountUser.mvUnitId != oldUserBase.mvUnitId 
                    || accountUser.mvGroupId != oldUserBase.mvGroupId || oldUserBase.mvRoleName != accountUser.mvRoleName){
                        return res.json(utils.response(0, ` It has been rejected. MV information cannot be edited!`));
                }
            }
        } 
        
        if(oldUserBase.cvUserId && !accountUser.cvRole) {
            return res.json(utils.response(0, ` It has been approved, CV Account Type Request can not be empty!`));
        }
        if(oldUserBase.mvUserId && !accountUser.mvUserType) {
            return res.json(utils.response(0, ` It has been approved, MV Account Type Request can not be empty!`));
        }
        let newNric = ((accountUser.nric).toString()).substr(0, 1)+((accountUser.nric).toString()).substr(((accountUser.nric).toString()).length-4, 4);
        let password = utils.generateMD5Code((accountUser.nric.substr((accountUser.nric.length)-4, 4)) + accountUser.contactNumber.substr(0, 4)).toUpperCase();
        accountUser.loginName = newNric + ((accountUser.fullName.toString()).replace(/\s*/g,"").toUpperCase()).substr(0, 3);
        let userBaseStatus = await TaskUtils.getUserByData(userBaseId, accountUser.nric, accountUser.contactNumber, accountUser.loginName, accountUser.fullName)
        if(userBaseStatus) throw userBaseStatus
        accountUser.creator = oldUserBase.creator ? oldUserBase.creator : req.cookies.userId
        // if(!accountUser.creator) accountUser.creator = oldUserBase.cvUserId ? oldUserBase.cvUserId : oldUserBase.mvUserId

        let errData = await TaskUtils.initCVAndMVUser(accountUser, optionType, userBaseId, req.cookies.userId, req.cookies.userType)
        if(errData) throw ` Edit failure.`
        return res.json(utils.response(1, true));                 
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.changeSelfPassword = async function (req, res) {
    let cvTransaction= null;
    let mvTransaction = null;
    try {
        let userId = req.cookies.userId;
        let { oldPassword, newPassword, confirmPassword } = req.body;
        let user = await User.findByPk(userId);
        if (!user) {
            return res.json(utils.response(0, 'User does not exist'));
        }
        let loginUserBase = await UserBase.findOne({ where: { mvUserId: req.cookies.userId } });

        let oldMd5Password = utils.generateMD5Code(oldPassword).toUpperCase();
        if (oldMd5Password != user.password) {
            return res.json(utils.response(0, 'Old Password is not correct.'));
        }

        mvTransaction = await sequelizeObj.transaction();
        let md5Password = utils.generateMD5Code(newPassword);
        await User.update({
            password: md5Password.toUpperCase(),
            lastChangePasswordDate: moment().format('YYYY-MM-DD HH:mm:ss')
        }, { where: {userId: userId}, transaction: mvTransaction });

        await OperationRecord.create({
            operatorId: userId,
            operatorName: user.fullName,
            businessType: 'Manage User',
            businessId: userId,
            optType: 'Change Password',
            beforeData: oldMd5Password,
            afterData: md5Password.toUpperCase(),
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: 'User change self password on first login.'
        }, { transaction: mvTransaction })

        let userBase = await UserBase.findOne({where: {mvUserId: userId}});
        if (userBase) {
            await UserBase.update({
                password: md5Password.toUpperCase(),
                updatedAt: moment().format('YYYY-MM-DD HH:mm:ss')
            }, {where: {id: userBase.id}, transaction: mvTransaction });
            let cvUserId = userBase.cvUserId;
            if (cvUserId) {
                cvTransaction = await sequelizeSystemObj.transaction();
                await _SysUser.USER.update({
                    password: md5Password,
                    lastChangePasswordDate: moment().format('YYYY-MM-DD HH:mm:ss')
                }, { where: {id: cvUserId}, transaction: cvTransaction });
                await _SysUser.UserManagementReport.create({
                    userId: cvUserId,
                    operatorUserBaseId: loginUserBase ? loginUserBase.id : null,
                    remark: 'Change password by server.',
                    operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                    activity: "Change Password",
                    triggeredBy: user.fullName
                }, { transaction: cvTransaction });
            }
        }
        if (cvTransaction) {
            await cvTransaction.commit();
        }
        if (mvTransaction) {
            await mvTransaction.commit();
        }

        res.clearCookie('needChangePassword');
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        if (cvTransaction) {
            await cvTransaction.rollback();
        }
        if (mvTransaction) {
            await mvTransaction.rollback();
        }
        return res.json(utils.response(0, "System error, please view error log file!"));
    }
}

module.exports.changeUserPassword = async function (req, res) {
    let cvTransaction = null;
    let mvTransaction = null;
    try {
        let userId = req.cookies.userId;
        let { editUserBaseId, newPassword } = req.body;
        let loginUser = await User.findByPk(userId);
        if (!loginUser) {
            return res.json(utils.response(0, 'Login User does not exist'));
        }
        let userBase = await UserBase.findByPk(editUserBaseId);
        if (!userBase) {
            return res.json(utils.response(0, 'Current edit user does not exist'));
        }

        let mvUserId = userBase.mvUserId;
        let cvUserId = userBase.cvUserId;

        mvTransaction = await sequelizeObj.transaction();
        let md5Password = utils.generateMD5Code(newPassword);
        await UserBase.update({
            password: md5Password.toUpperCase(),
            updatedAt: moment().format('YYYY-MM-DD HH:mm:ss')
        }, {where: {id: editUserBaseId}, transaction: mvTransaction });

        if (mvUserId) {
            await User.update({
                password: md5Password.toUpperCase(),
                lastChangePasswordDate: moment().format('YYYY-MM-DD HH:mm:ss')
            }, { where: {userId: mvUserId}, transaction: mvTransaction });
    
            await OperationRecord.create({
                operatorId: userId,
                businessType: 'Manage User',
                businessId: mvUserId,
                optType: 'Change Password',
                beforeData: userBase.password,
                afterData: md5Password.toUpperCase(),
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'Manager change user password.'
            }, { transaction: mvTransaction });
        }
        
        if (cvUserId) {
            cvTransaction = await sequelizeSystemObj.transaction();
            await _SysUser.USER.update({
                password: md5Password,
                lastChangePasswordDate: moment().format('YYYY-MM-DD HH:mm:ss')
            }, { where: {id: cvUserId}, transaction: cvTransaction });
            await _SysUser.UserManagementReport.create({
                userId: cvUserId,
                remark: 'Change password by server.',
                operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                activity: "Change Password",
                triggeredBy: userBase.fullName
            }, { transaction: cvTransaction });
        }

        if (cvTransaction) {
            await cvTransaction.commit();
        }
        if (mvTransaction) {
            await mvTransaction.commit();
        }

        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        if (cvTransaction) {
            await cvTransaction.rollback();
        }
        if (mvTransaction) {
            await mvTransaction.rollback();
        }
        return res.json(utils.response(0, error));
    }
}

module.exports.resetUserPassword = async function (req, res) {
    let cvTransaction = null;
    let mvTransaction = null;
    try {
        let userId = req.cookies.userId;
        let userBaseId = req.body.applyId;
        let loginUser = await User.findByPk(userId);
        if (!loginUser) {
            return res.json(utils.response(0, 'Login User does not exist'));
        }
        let loginUserBase = await UserBase.findOne({ where: { mvUserId: req.cookies.userId } });

        let userBase = await UserBase.findByPk(userBaseId);
        if (!userBase) {
            return res.json(utils.response(0, 'Current edit user does not exist'));
        }

        let mvUserId = userBase.mvUserId;
        let cvUserId = userBase.cvUserId;

        let initPassword = utils.generateMD5Code((userBase.nric.substr((userBase.nric.length)-4, 4)) + userBase.contactNumber.substr(0, 4)).toLowerCase();

        mvTransaction = await sequelizeObj.transaction();
        await UserBase.update({
            password: initPassword.toUpperCase(),
            updatedAt: moment().format('YYYY-MM-DD HH:mm:ss')
        }, {where: {id: userBaseId}, transaction: mvTransaction });

        if (mvUserId) {
            await User.update({
                password: initPassword.toUpperCase(),
                lastChangePasswordDate: null
            }, { where: {userId: mvUserId}, transaction: mvTransaction });
    
            await OperationRecord.create({
                operatorId: userId,
                operatorName: loginUser.fullName,
                businessType: 'Manage User',
                businessId: userBaseId,
                optType: 'Reset Password',
                beforeData: userBase.password,
                afterData: initPassword.toUpperCase(),
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'Manager change user password.'
            }, { transaction: mvTransaction });
        }
        
        if (cvUserId) {
            cvTransaction = await sequelizeSystemObj.transaction();
            await _SysUser.USER.update({
                password: initPassword,
                lastChangePasswordDate: null
            }, { where: {id: cvUserId}, transaction: cvTransaction });
            await _SysUser.UserManagementReport.create({
                userId: cvUserId,
                operatorUserBaseId: loginUserBase ? loginUserBase.id : null,
                remark: 'Reset password by server.',
                operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                activity: "Reset Password",
                triggeredBy: loginUser.fullName,
            }, { transaction: cvTransaction });
        }

        if (cvTransaction) {
            await cvTransaction.commit();
        }
        if (mvTransaction) {
            await mvTransaction.commit();
        }

        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        if (cvTransaction) {
            await cvTransaction.rollback();
        }
        if (mvTransaction) {
            await mvTransaction.rollback();
        }
        return res.json(utils.response(0, error));
    }
}

module.exports.unLockUser = async function (req, res) {
    let cvTransaction = null;
    let mvTransaction = null;
    try {
        let userId = req.cookies.userId;
        let userBaseId = req.body.applyId;
        let loginUser = await User.findByPk(userId);
        if (!loginUser) {
            return res.json(utils.response(0, 'Login User does not exist'));
        }
        let loginUserBase = await UserBase.findOne({ where: { mvUserId: req.cookies.userId } });

        let userBase = await UserBase.findByPk(userBaseId);
        if (!userBase) {
            return res.json(utils.response(0, 'Current unlock user does not exist'));
        }

        let mvUserId = userBase.mvUserId;
        let cvUserId = userBase.cvUserId;
        if (mvUserId) {
            mvTransaction = await sequelizeObj.transaction();

            await User.update({
                pwdErrorTimes: 0,
                unLockTime: moment().format('YYYY-MM-DD HH:mm:ss')
            }, { where: {userId: mvUserId}, transaction: mvTransaction });
    
            await OperationRecord.create({
                operatorId: userId,
                operatorName: loginUser.fullName,
                businessType: 'Manage User',
                businessId: userBaseId,
                optType: 'Unlocked',
                beforeData: 'Lock Out',
                afterData: 'Unlock',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'Manager unlock user.'
            }, { transaction: mvTransaction });
        }
        
        if (cvUserId) {
            cvTransaction = await sequelizeSystemObj.transaction();
            await _SysUser.USER.update({
                times: 0,
                status: 'Active',
                activeTime: moment().format('YYYY-MM-DD HH:mm:ss')
            }, { where: {id: cvUserId}, transaction: cvTransaction });

            await _SysUser.UserManagementReport.create({
                userId: cvUserId,
                operatorUserBaseId: loginUserBase ? loginUserBase.id : null,
                remark: 'Unlock by server.',
                operateDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                activity: "Unlocked",
                triggeredBy: loginUser.fullName
            }, { transaction: cvTransaction });
        }

        if (cvTransaction) {
            await cvTransaction.commit();
        }
        if (mvTransaction) {
            await mvTransaction.commit();
        }

        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        if (cvTransaction) {
            await cvTransaction.rollback();
        }
        if (mvTransaction) {
            await mvTransaction.rollback();
        }
        return res.json(utils.response(0, error));
    }
}

module.exports.changeSelfEmail = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let email = req.body.email;
        let loginUser = await User.findByPk(userId);
        if (!loginUser) {
            log.error('Login User does not exist.')
            return res.json(utils.response(0, 'Login User does not exist.'));
        }
        let userBase = await UserBase.findOne({ where: { mvUserId: userId} });
        if (!userBase) {
            log.error('Login User data error.')
            return res.json(utils.response(0, 'Login User data error.'));
        }

        await UserBase.update({ email }, {where : {mvUserId: userId}});

        let cvUserId = userBase.cvUserId;
        if (cvUserId) {
            await _SysUser.USER.update({ email }, {where: {id: cvUserId} });
        }

        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserOptHistoryList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let userBaseId = req.body.userBaseId;
        let loginUser = await User.findByPk(userId);
        if (!loginUser) {
            return res.json(utils.response(0, 'Login User does not exist.'));
        }
        let userBase = await UserBase.findByPk(userBaseId);
        if (!userBase || !userBase.mvUserId) {
            return res.json(utils.response(0, 'User data error.'));
        }
        let mvUserId = userBase.mvUserId;

        let userOptList = await sequelizeObj.query(`
            SELECT
                o.optType, o.optTime, o.operatorId, o.operatorName, us.fullname
            FROM operation_record o
            LEFT JOIN user us on o.operatorId = us.userId
            WHERE businessType = 'Manage User' AND businessId = ? order by o.optTime desc
        `, { type: QueryTypes.SELECT, replacements: [userBaseId] })

        return res.json(utils.response(1, userOptList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getHqTypeList = async function(req, res){
    try {
        let userId = req.body.userStatus == true ? req.cookies ? req.cookies.userId : null : null;
        let userBaseUsableId = req.body.userUsableId
        let sql = `SELECT hq FROM unit WHERE hq is not null`
        let replacements = []
        if(userId){
            let user = await User.findOne({ where: { userId: userId } })
            if(!user) throw ` The current user does not exist.`
            if(user.hq) {
                sql += ` and FIND_IN_SET(?, hq)`
                replacements.push(user.hq)
            } 
        } 
        sql += ` group by hq order by hq`
        let hqTypeList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        let newHqTypeList = []
        for(let item of hqTypeList){
            let data = item.hq.split(',')
            data = data.filter(item => item)
            newHqTypeList = [...newHqTypeList, ...data]
        }
        newHqTypeList = Array.from(new Set(newHqTypeList))

        let userList = []
        if(!userBaseUsableId || !userId){
            userList = await UserBase.findAll({ where: { hq: { [Op.not]: null } } })
        } else if(userBaseUsableId) {
            userList = await UserBase.findAll({ where: { hq: { [Op.not]: null }, id: { [Op.not]: userBaseUsableId } } })
        }
        if(userList.length > 0){
            let hqList = Array.from(new Set(userList.map(item => item.hq)))
            log.warn(` hq that has been used ==> ${ JSON.stringify(hqList) }`)

            // Attention: 
            newHqTypeList = newHqTypeList.filter(item => !hqList.includes(item))
            log.warn(` Available hq ==> ${ JSON.stringify(newHqTypeList) }`)
        }
        return res.json(utils.response(1, newHqTypeList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserBaseByUserId = async function (req, res) {
    try {
        let cvmvuserId = req.body.cvmvuserId;
        let dataType = req.body.dataType;
        let userBase = null;
        if(dataType == 'cv') {
            userBase = await UserBase.findOne({ where: { cvUserId: cvmvuserId } })
        } else if(dataType == 'mv'){
            userBase = await UserBase.findOne({ where: { mvUserId: cvmvuserId } })
        }
        let mvUnit = null;
        if(userBase) {
            if(userBase.mvUnitId) {
                let unit = await Unit.findOne({ where: { id: userBase.mvUnitId } })
                if(unit) {
                    mvUnit = { hub: unit.unit, node: unit.subUnit };
                }
            }
            if (userBase.cvUserId) {
                let cvUser = await _SysUser.USER.findByPk(userBase.cvUserId);
                let cvUserStatus = cvUser ? cvUser.status : '';
                userBase.dataValues.cvUserStatus = cvUserStatus;
            }
        }
        return res.json(utils.response(1, { data: userBase, hubNode: mvUnit }));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.skipCVUrl = async function (req, res) {
    try {
        let dataList = JSON.stringify({ dataFrom: 'server', userId: req.cookies.userId })
        let str = utils.generateAESCode(dataList)
        let url = `/user/registerUser?registerFrom=${str}`
        return res.json(utils.response(1, url));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.urlParameterDecode = async function (req, res) {
    try {
        let str = req.body.str;
        let dataList = utils.decodeAESCode(str)
        dataList = JSON.parse(dataList)
        return res.json(utils.response(1, dataList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}